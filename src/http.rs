use crate::types::RunConfig;
use anyhow::{bail, Context, Result};
use futures::StreamExt;
use reqwest::{redirect::Policy, Client, Method, Url};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct HttpRecorder {
    client: Client,
    root: PathBuf,
    config: RunConfig,
    sequence: Arc<AtomicU64>,
    semaphore: Arc<Semaphore>,
    allow_loopback: bool,
    limits: Option<Arc<RequestLimits>>,
}

struct RequestLimits {
    max_requests: u64,
    max_response_bytes: u64,
    deadline: Instant,
    counters: Mutex<(u64, u64)>,
}

/// What a request does once it holds its concurrency permit: first `stop` may cancel it (the
/// request then fails with `NotSent`), then `notify` learns that it is about to be sent.
#[derive(Clone, Copy, Default)]
pub struct SendGate<'a> {
    pub stop: Option<&'a (dyn Fn() -> bool + Sync)>,
    pub notify: Option<&'a tokio::sync::Notify>,
}

/// The request was stopped before anything was sent.
#[derive(Debug)]
pub struct NotSent;

impl std::fmt::Display for NotSent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("The request was stopped before it was sent")
    }
}

impl std::error::Error for NotSent {}

#[derive(Clone, Debug)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub artifact: String,
    pub headers: std::collections::BTreeMap<String, String>,
}

impl HttpResponse {
    pub fn json(&self) -> Result<Value> {
        serde_json::from_slice(&self.body)
            .context("Response is not valid JSON; see the raw response artifact")
    }
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

fn sensitive(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    [
        "authorization",
        "cookie",
        "secret",
        "token",
        "password",
        "api-key",
        "api_key",
        "apikey",
        "credential",
    ]
    .iter()
    .any(|s| key.contains(s))
}

fn safe_body(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        if sensitive(key) {
                            json!("[REDACTED]")
                        } else {
                            safe_body(value)
                        },
                    )
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.iter().map(safe_body).collect()),
        value => value.clone(),
    }
}

fn safe_url(url: &Url) -> String {
    let mut safe = url.clone();
    let pairs: Vec<_> = url
        .query_pairs()
        .map(|(key, value)| {
            let value = if sensitive(&key) {
                "[REDACTED]".to_owned()
            } else {
                value.into_owned()
            };
            (key.into_owned(), value)
        })
        .collect();
    if !pairs.is_empty() {
        safe.query_pairs_mut().clear().extend_pairs(pairs);
    }
    safe.set_fragment(None);
    safe.to_string()
}

fn write_metadata(path: &Path, value: &Value) -> Result<()> {
    // Async filesystem writes can outlive a canceled future. Keep each small
    // metadata replacement synchronous and atomic to prevent competing writes.
    let temporary = path.with_extension("json.tmp");
    std::fs::write(&temporary, serde_json::to_vec(value)?)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

/// Finalizes one raw HTTP record on every exit, including a cancelled request future: it gzips
/// the retained body, hashes the exact bytes, and only then points the metadata at `.body.gz`.
struct RawRecord {
    /// False for a light record: nothing is written, and the body stays in memory only.
    enabled: bool,
    root: PathBuf,
    prefix: String,
    metadata: Value,
    finished: bool,
}

impl RawRecord {
    fn finish(&mut self) -> Result<()> {
        self.finished = true;
        if !self.enabled {
            return Ok(());
        }
        let streamed = self.root.join(format!("{}.body", self.prefix));
        if streamed.is_file() {
            let bytes = std::fs::read(&streamed)?;
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            std::io::Write::write_all(&mut encoder, &bytes)?;
            let compressed = format!("{}.body.gz", self.prefix);
            let temporary = self.root.join(format!("{compressed}.tmp"));
            std::fs::write(&temporary, encoder.finish()?)?;
            std::fs::rename(&temporary, self.root.join(&compressed))?;
            self.metadata["body_artifact"] = json!(compressed);
            self.metadata["body_encoding"] = json!("gzip");
            self.metadata["body_sha256"] = json!(format!("{:x}", Sha256::digest(&bytes)));
            write_metadata(
                &self.root.join(format!("{}.json", self.prefix)),
                &self.metadata,
            )?;
            std::fs::remove_file(&streamed)?;
        } else {
            self.metadata["body_artifact"] = Value::Null;
            write_metadata(
                &self.root.join(format!("{}.json", self.prefix)),
                &self.metadata,
            )?;
        }
        Ok(())
    }
}

impl Drop for RawRecord {
    fn drop(&mut self) {
        if !self.finished {
            if self.metadata["complete"] != true && self.metadata["failure"].is_null() {
                self.metadata["failure"] = json!("request cancelled before the response ended");
            }
            let _ = self.finish();
        }
    }
}

impl HttpRecorder {
    #[cfg(test)]
    pub(crate) fn loopback_for_test(run_dir: &Path, config: &RunConfig) -> Result<Self> {
        let mut recorder = Self::new_bounded(run_dir, config, 64, 1024 * 1024, 10)?;
        // Offline tests must not inherit a proxy or relax production URL rules.
        recorder.client = Client::builder()
            .no_proxy()
            .redirect(Policy::none())
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()?;
        recorder.allow_loopback = true;
        Ok(recorder)
    }

    /// A clone with its own request limit that shares the run folder and the raw-file sequence.
    pub fn with_concurrency(&self, concurrency: usize) -> Self {
        let mut clone = self.clone();
        clone.semaphore = Arc::new(Semaphore::new(concurrency.max(1)));
        clone
    }

    pub fn run_dir(&self) -> &Path {
        &self.root
    }

    pub fn new(run_dir: &Path, config: &RunConfig) -> Result<Self> {
        if config.full_record {
            std::fs::create_dir_all(run_dir.join("raw"))?;
        }
        Ok(Self {
            client: Client::builder()
                .redirect(Policy::none())
                .timeout(Duration::from_secs(config.timeout_secs))
                .build()?,
            root: run_dir.to_path_buf(),
            config: config.clone(),
            sequence: Arc::new(AtomicU64::new(0)),
            semaphore: Arc::new(Semaphore::new(config.concurrency)),
            allow_loopback: false,
            limits: None,
        })
    }

    /// Limits cover every clone, including connector calls and Jev requests. Tests only.
    #[cfg(test)]
    pub fn new_bounded(
        run_dir: &Path,
        config: &RunConfig,
        max_requests: u64,
        max_response_bytes: u64,
        deadline_secs: u64,
    ) -> Result<Self> {
        anyhow::ensure!(
            max_requests > 0 && max_response_bytes > 0 && deadline_secs > 0,
            "HTTP limits must be positive"
        );
        let mut recorder = Self::new(run_dir, config)?;
        recorder.limits = Some(Arc::new(RequestLimits {
            max_requests,
            max_response_bytes,
            deadline: Instant::now()
                .checked_add(Duration::from_secs(deadline_secs))
                .context("HTTP deadline is too large")?,
            counters: Mutex::new((0, 0)),
        }));
        Ok(recorder)
    }

    #[cfg(test)]
    pub fn metrics(&self) -> Value {
        match &self.limits {
            Some(limits) => {
                let (requests, retained) =
                    *limits.counters.lock().unwrap_or_else(|e| e.into_inner());
                json!({"bounded":true,"requests_started":requests,"retained_response_bytes":retained,
                    "max_requests":limits.max_requests,"max_response_bytes":limits.max_response_bytes,
                    "deadline_reached":Instant::now() >= limits.deadline,
                    "scope":"Source and Jev HTTP attempts. Bytes count retained response bodies, not transport bytes."})
            }
            None => json!({"bounded":false}),
        }
    }

    pub async fn request(
        &self,
        method: Method,
        url: &str,
        headers: Vec<(String, String)>,
        body: Option<Value>,
    ) -> Result<HttpResponse> {
        self.request_recorded(method, url, headers, body, true, SendGate::default())
            .await
    }

    /// For a caller that saves the exact request body in its own audit record (the Jev trace).
    /// The metadata keeps the body's SHA-256 and size, so the one full copy stays verifiable.
    /// `gate` acts once the request holds its concurrency permit: it can notify the caller, so the
    /// caller times the request and not its queue wait, or stop the request before it is sent.
    pub async fn request_body_recorded_elsewhere(
        &self,
        method: Method,
        url: &str,
        headers: Vec<(String, String)>,
        body: Option<Value>,
        gate: SendGate<'_>,
    ) -> Result<HttpResponse> {
        self.request_recorded(method, url, headers, body, false, gate)
            .await
    }

    async fn request_recorded(
        &self,
        method: Method,
        url: &str,
        headers: Vec<(String, String)>,
        body: Option<Value>,
        record_body: bool,
        gate: SendGate<'_>,
    ) -> Result<HttpResponse> {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let prefix = format!("raw/{sequence:06}");
        let metadata_path = self.root.join(format!("{prefix}.json"));
        // The body streams to `.body`. RawRecord gzips it to `.body.gz` when the request ends,
        // including cancellation, and only then points the metadata at the compressed file.
        let streaming_artifact = format!("{prefix}.body");
        let mut record = RawRecord {
            enabled: self.config.full_record,
            root: self.root.clone(),
            prefix: prefix.clone(),
            metadata: json!({"method": method.as_str(), "body_artifact": streaming_artifact,
                "body_encoding": "identity", "complete": false}),
            finished: false,
        };
        let started = Instant::now();
        let operation = async {
            let parsed = Url::parse(url).context("Invalid request URL")?;
            if !parsed.username().is_empty() || parsed.password().is_some() {
                bail!("URL credentials are forbidden");
            }
            record.metadata["url"] = json!(safe_url(&parsed));
            record.metadata["request_header_names"] =
                json!(headers.iter().map(|(key, _)| key).collect::<Vec<_>>());
            // Only explicit safe headers retain values. Unknown headers can contain credentials.
            record.metadata["request_headers"] = json!(headers
                .iter()
                .map(|(key, value)| {
                    let safe = matches!(
                        key.to_ascii_lowercase().as_str(),
                        "accept" | "content-type" | "user-agent"
                    );
                    (
                        key.clone(),
                        if safe {
                            value.clone()
                        } else {
                            "[REDACTED]".into()
                        },
                    )
                })
                .collect::<std::collections::BTreeMap<_, _>>());
            if record_body {
                record.metadata["request_body"] =
                    body.as_ref().map(safe_body).unwrap_or(Value::Null);
            } else if let Some(body) = &body {
                let bytes = serde_json::to_vec(body)?;
                record.metadata["request_body_sha256"] =
                    json!(format!("{:x}", Sha256::digest(&bytes)));
                record.metadata["request_body_bytes"] = json!(bytes.len());
                record.metadata["request_body_recorded_in"] = json!("caller audit record");
            }
            if self.config.fixture {
                bail!("Network requests are forbidden in fixture mode");
            }
            if parsed.scheme() != "https"
                && !(self.allow_loopback
                    && parsed.scheme() == "http"
                    && parsed.host_str() == Some("127.0.0.1"))
            {
                bail!("Request URL must use HTTPS");
            }
            if record.enabled {
                write_metadata(&metadata_path, &record.metadata)?;
            }
            let _permit = self.semaphore.acquire().await?;
            if gate.stop.is_some_and(|stop| stop()) {
                return Err(NotSent.into());
            }
            if let Some(sent) = gate.notify {
                sent.notify_one();
            }
            // Wall-clock start after the permit, so timelines separate queue wait from transfer.
            record.metadata["queued_ms"] = json!(started.elapsed().as_millis() as u64);
            record.metadata["started_unix_ms"] = json!(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64);
            if let Some(limits) = &self.limits {
                let mut counters = limits.counters.lock().unwrap_or_else(|e| e.into_inner());
                anyhow::ensure!(
                    Instant::now() < limits.deadline,
                    "HTTP session deadline reached"
                );
                anyhow::ensure!(
                    counters.0 < limits.max_requests,
                    "HTTP request limit reached"
                );
                anyhow::ensure!(
                    counters.1 < limits.max_response_bytes,
                    "HTTP response byte limit reached"
                );
                counters.0 += 1;
            }
            let mut request = self.client.request(method, parsed);
            for (key, value) in headers {
                request = request.header(key, value);
            }
            if let Some(body) = body {
                request = request.json(&body);
            }
            let response = request
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("HTTP request failed: {}", e.without_url()))?;
            let status = response.status().as_u16();
            record.metadata["status"] = json!(status);
            let response_headers: std::collections::BTreeMap<String, String> = response
                .headers()
                .iter()
                .filter(|(key, _)| {
                    matches!(
                        key.as_str(),
                        "content-type"
                            | "content-length"
                            | "retry-after"
                            | "retry-after-ms"
                            | "date"
                            | "etag"
                            | "last-modified"
                            | "x-request-id"
                            | "cf-ray"
                    )
                })
                .map(|(key, value)| {
                    (
                        key.as_str().to_owned(),
                        value.to_str().unwrap_or("[BINARY]").to_owned(),
                    )
                })
                .collect();
            record.metadata["response_headers"] = json!(response_headers);
            // Synchronous chunk writes: a cancelled request leaves no write pending.
            let mut file = if record.enabled {
                Some(std::fs::File::create(self.root.join(&streaming_artifact))?)
            } else {
                None
            };
            let mut stream = response.bytes_stream();
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk
                    .map_err(|e| anyhow::anyhow!("Response body failed: {}", e.without_url()))?;
                let available = self.config.max_body_bytes.saturating_sub(bytes.len());
                let mut retain_count = chunk.len().min(available);
                if let Some(limits) = &self.limits {
                    let mut counters = limits.counters.lock().unwrap_or_else(|e| e.into_inner());
                    retain_count = retain_count
                        .min(limits.max_response_bytes.saturating_sub(counters.1) as usize);
                    counters.1 += retain_count as u64;
                }
                let retained = &chunk[..retain_count];
                if let Some(file) = file.as_mut() {
                    std::io::Write::write_all(file, retained)?;
                }
                bytes.extend_from_slice(retained);
                record.metadata["retained_bytes"] = json!(bytes.len());
                if retain_count < chunk.len().min(available) {
                    if let Some(file) = file.as_mut() {
                        std::io::Write::flush(file)?;
                    }
                    bail!("HTTP response byte limit reached; retained body is incomplete");
                }
                if chunk.len() > available {
                    if let Some(file) = file.as_mut() {
                        std::io::Write::flush(file)?;
                    }
                    bail!(
                        "Response exceeds --max-body-bytes {}; retained body is incomplete",
                        self.config.max_body_bytes
                    );
                }
            }
            if let Some(file) = file.as_mut() {
                std::io::Write::flush(file)?;
            }
            record.metadata["complete"] = json!(true);
            if (300..400).contains(&status) {
                bail!("HTTP redirect {status} refused; credentials were not forwarded");
            }
            Ok(HttpResponse {
                status,
                body: bytes,
                artifact: streaming_artifact.clone(),
                headers: response_headers,
            })
        };
        let result: Result<HttpResponse> = if let Some(limits) = &self.limits {
            match tokio::time::timeout_at(limits.deadline.into(), operation).await {
                Ok(result) => result,
                Err(_) => Err(anyhow::anyhow!(
                    "HTTP session deadline reached; response may be incomplete"
                )),
            }
        } else {
            operation.await
        };
        record.metadata["elapsed_ms"] = json!(started.elapsed().as_millis());
        if let Err(error) = &result {
            record.metadata["failure"] = json!(error.to_string());
        }
        record.finish()?;
        result.map(|mut response| {
            if let Some(artifact) = record.metadata["body_artifact"].as_str() {
                response.artifact = artifact.to_owned();
            }
            response
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw_body(path: &Path) -> Vec<u8> {
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(
            &mut flate2::read::GzDecoder::new(std::fs::File::open(path).unwrap()),
            &mut bytes,
        )
        .unwrap();
        bytes
    }
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    async fn server(reply: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 4096];
            let bytes_read = socket.read(&mut request).await.unwrap();
            assert!(bytes_read > 0, "The server received an empty request");
            socket.write_all(reply.as_bytes()).await.unwrap();
        });
        format!("http://{addr}")
    }
    #[tokio::test]
    async fn bounded_clones_share_request_and_byte_limits() {
        let dir = tempfile::tempdir().unwrap();
        let mut recorder =
            HttpRecorder::new_bounded(dir.path(), &RunConfig::default(), 1, 4, 30).unwrap();
        recorder.allow_loopback = true;
        let clone = recorder.clone();
        let url = server("HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nabcdef").await;
        let error = recorder
            .request(Method::GET, &url, vec![], None)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("byte limit"));
        assert_eq!(raw_body(&dir.path().join("raw/000000.body.gz")), b"abcd");
        let error = clone
            .request(Method::GET, &url, vec![], None)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("request limit"));
        assert_eq!(clone.metrics()["requests_started"], 1);
        assert_eq!(clone.metrics()["retained_response_bytes"], 4);
        let metadata: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("raw/000000.json")).unwrap())
                .unwrap();
        assert_eq!(metadata["complete"], false);
    }

    #[tokio::test]
    async fn expired_session_refuses_network_and_records_failure() {
        let dir = tempfile::tempdir().unwrap();
        let mut recorder =
            HttpRecorder::new_bounded(dir.path(), &RunConfig::default(), 2, 100, 1).unwrap();
        Arc::get_mut(recorder.limits.as_mut().unwrap())
            .unwrap()
            .deadline = Instant::now() - Duration::from_secs(1);
        let error = recorder
            .request(Method::GET, "https://example.invalid", vec![], None)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("deadline"));
        assert_eq!(recorder.metrics()["requests_started"], 0);
        let metadata: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("raw/000000.json")).unwrap())
                .unwrap();
        assert!(metadata["failure"].as_str().unwrap().contains("deadline"));
    }

    #[tokio::test]
    async fn concurrent_clones_cannot_exceed_request_allowance() {
        let dir = tempfile::tempdir().unwrap();
        let mut recorder =
            HttpRecorder::new_bounded(dir.path(), &RunConfig::default(), 1, 100, 30).unwrap();
        recorder.allow_loopback = true;
        let clone = recorder.clone();
        let url = server("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok").await;
        let (a, b) = tokio::join!(
            recorder.request(Method::GET, &url, vec![], None),
            clone.request(Method::GET, &url, vec![], None)
        );
        assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
        assert_eq!(recorder.metrics()["requests_started"], 1);
        assert_eq!(recorder.metrics()["retained_response_bytes"], 2);
    }

    #[tokio::test]
    async fn deadline_preserves_a_partially_received_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            assert!(socket.read(&mut request).await.unwrap() > 0);
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nabc")
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
        });
        let directory = tempfile::tempdir().unwrap();
        let mut recorder =
            HttpRecorder::new_bounded(directory.path(), &RunConfig::default(), 2, 100, 1).unwrap();
        recorder.allow_loopback = true;
        let error = recorder
            .request(Method::GET, &format!("http://{address}"), vec![], None)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("deadline"));
        assert_eq!(
            raw_body(&directory.path().join("raw/000000.body.gz")),
            b"abc"
        );
        let metadata: Value = serde_json::from_slice(
            &std::fs::read(directory.path().join("raw/000000.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(metadata["complete"], false);
        assert_eq!(metadata["retained_bytes"], 3);
        assert!(metadata["failure"].as_str().unwrap().contains("deadline"));
        task.abort();
    }
    #[tokio::test]
    async fn a_cancelled_request_still_finalizes_its_partial_body_and_metadata() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            assert!(socket.read(&mut request).await.unwrap() > 0);
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nabc")
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        let directory = tempfile::tempdir().unwrap();
        let mut recorder = HttpRecorder::new(directory.path(), &RunConfig::default()).unwrap();
        recorder.allow_loopback = true;
        let url = format!("http://{address}");
        let request = {
            let recorder = recorder.clone();
            tokio::spawn(async move { recorder.request(Method::GET, &url, vec![], None).await })
        };
        // Wait until the first bytes are on disk, then cancel from outside, as a fetch deadline does.
        let streamed = directory.path().join("raw/000000.body");
        for _ in 0..200 {
            if std::fs::metadata(&streamed)
                .map(|m| m.len() == 3)
                .unwrap_or(false)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        let metadata: Value = serde_json::from_slice(
            &std::fs::read(directory.path().join("raw/000000.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(metadata["body_artifact"], "raw/000000.body.gz");
        assert_eq!(metadata["body_encoding"], "gzip");
        assert_eq!(metadata["complete"], false);
        assert!(metadata["failure"].as_str().unwrap().contains("cancelled"));
        let body = raw_body(&directory.path().join("raw/000000.body.gz"));
        assert_eq!(body, b"abc");
        assert_eq!(
            metadata["body_sha256"],
            format!("{:x}", Sha256::digest(&body))
        );
        assert!(!streamed.exists(), "the uncompressed copy is removed");
        server.abort();
    }
    #[tokio::test]
    async fn preserves_retry_after_ms_and_filters_unsafe_response_headers() {
        let dir = tempfile::tempdir().unwrap();
        let mut recorder = HttpRecorder::new(dir.path(), &RunConfig::default()).unwrap();
        recorder.allow_loopback = true;
        let url = server("HTTP/1.1 429 Too Many Requests\r\nRetry-After-Ms: 250\r\nRetry-After: 2\r\nSet-Cookie: private=value\r\nX-Unknown-Secret: private\r\nContent-Length: 4\r\n\r\nwait").await;
        let response = recorder
            .request(Method::GET, &url, vec![], None)
            .await
            .unwrap();
        assert_eq!(response.status, 429);
        assert_eq!(
            response.headers.get("retry-after-ms").map(String::as_str),
            Some("250")
        );
        assert_eq!(
            response.headers.get("retry-after").map(String::as_str),
            Some("2")
        );
        assert!(!response.headers.contains_key("set-cookie"));
        assert!(!response.headers.contains_key("x-unknown-secret"));
        let metadata: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("raw/000000.json")).unwrap())
                .unwrap();
        assert_eq!(metadata["response_headers"]["retry-after-ms"], "250");
    }

    #[tokio::test]
    async fn preserves_error_body_and_redacts_request_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let mut recorder = HttpRecorder::new(dir.path(), &RunConfig::default()).unwrap();
        recorder.allow_loopback = true;
        let url =
            server("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 14\r\n\r\nupstream error")
                .await;
        let response = recorder
            .request(
                Method::GET,
                &format!("{url}?api_key=secret"),
                vec![("authorization".into(), "Bearer secret".into())],
                None,
            )
            .await
            .unwrap();
        assert_eq!(response.status, 503);
        assert_eq!(response.text(), "upstream error");
        let metadata = std::fs::read_to_string(dir.path().join("raw/000000.json")).unwrap();
        assert!(!metadata.contains("secret"));
        assert_eq!(
            raw_body(&dir.path().join(&response.artifact)),
            b"upstream error"
        );
    }
    #[tokio::test]
    async fn delayed_body_times_out_and_preserves_partial_response() {
        let dir = tempfile::tempdir().unwrap();
        let config = RunConfig {
            timeout_secs: 1,
            ..Default::default()
        };
        let mut recorder = HttpRecorder::new(dir.path(), &config).unwrap();
        recorder.allow_loopback = true;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let delayed = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            assert!(socket.read(&mut request).await.unwrap() > 0);
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nabc")
                .await
                .unwrap();
            socket.flush().await.unwrap();
            // Keep the connection open beyond both the request deadline and test guard.
            tokio::time::sleep(Duration::from_secs(10)).await;
            let _ = socket.write_all(b"def").await;
        });
        let attempt = tokio::time::timeout(
            Duration::from_secs(5),
            recorder.request(Method::GET, &url, vec![], None),
        )
        .await;
        delayed.abort();
        let error = attempt
            .expect("The recorder must enforce its own one-second deadline")
            .unwrap_err();
        assert!(error.to_string().contains("Response body failed"));
        let metadata: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("raw/000000.json")).unwrap())
                .unwrap();
        assert_eq!(metadata["status"], 200);
        assert_eq!(metadata["complete"], false);
        assert_eq!(metadata["retained_bytes"], 3);
        assert_eq!(metadata["failure"], error.to_string());
        assert_eq!(metadata["body_artifact"], "raw/000000.body.gz");
        assert_eq!(metadata["body_encoding"], "gzip");
        assert_eq!(raw_body(&dir.path().join("raw/000000.body.gz")), b"abc");
    }
    #[tokio::test]
    async fn malformed_json_preserves_complete_body_and_reports_parse_failure() {
        let dir = tempfile::tempdir().unwrap();
        let mut recorder = HttpRecorder::new(dir.path(), &RunConfig::default()).unwrap();
        recorder.allow_loopback = true;
        let url = server("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\n\r\n{\"items\": [").await;
        let response = recorder
            .request(Method::GET, &url, vec![], None)
            .await
            .unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"{\"items\": [");
        assert!(response
            .json()
            .unwrap_err()
            .to_string()
            .contains("Response is not valid JSON"));
        assert_eq!(
            raw_body(&dir.path().join(&response.artifact)),
            response.body
        );
        let metadata: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("raw/000000.json")).unwrap())
                .unwrap();
        assert_eq!(metadata["status"], 200);
        assert_eq!(metadata["complete"], true);
        assert_eq!(metadata["retained_bytes"], 11);
        assert_eq!(metadata["body_artifact"], response.artifact);
        // HTTP succeeded. Parsing fails separately and must not invent an empty result.
        assert!(metadata.get("failure").is_none());
    }
    #[tokio::test]
    async fn refuses_redirect_and_records_full_body() {
        let dir = tempfile::tempdir().unwrap();
        let mut recorder = HttpRecorder::new(dir.path(), &RunConfig::default()).unwrap();
        recorder.allow_loopback = true;
        let url = server("HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/leak\r\nContent-Length: 4\r\n\r\nmove").await;
        assert!(recorder
            .request(Method::GET, &url, vec![], None)
            .await
            .unwrap_err()
            .to_string()
            .contains("redirect"));
        assert_eq!(raw_body(&dir.path().join("raw/000000.body.gz")), b"move");
    }
    #[tokio::test]
    async fn oversized_body_is_an_explicit_recorded_failure() {
        let dir = tempfile::tempdir().unwrap();
        let config = RunConfig {
            max_body_bytes: 3,
            ..Default::default()
        };
        let mut recorder = HttpRecorder::new(dir.path(), &config).unwrap();
        recorder.allow_loopback = true;
        let url = server("HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nabcdef").await;
        assert!(recorder
            .request(Method::GET, &url, vec![], None)
            .await
            .unwrap_err()
            .to_string()
            .contains("max-body-bytes"));
        let metadata: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("raw/000000.json")).unwrap())
                .unwrap();
        assert_eq!(metadata["complete"], false);
        assert_eq!(raw_body(&dir.path().join("raw/000000.body.gz")), b"abc");
    }
}
