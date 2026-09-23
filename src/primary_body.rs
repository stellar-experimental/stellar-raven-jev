//! Opt-in exact primary-body recovery. The caller must supply a session-bound source.
//! This module does not score, rank, or replace the discovered document.

use anyhow::{ensure, Context, Result};
use futures::StreamExt;
use reqwest::{header::HeaderMap, redirect::Policy, Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const HOST: &str = "raw.githubusercontent.com";
const MAX_BYTES: usize = 1024 * 1024;
const TIMEOUT: Duration = Duration::from_secs(20);
const FIXTURE: &[u8] = b"# Synthetic primary-body fixture\n\nThis deterministic Markdown body is test evidence only.\nIt does not reproduce the discovered source.\n";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrimarySource {
    pub document_id: String,
    pub source_id: String,
    pub title: String,
    pub original_url: String,
    pub text_sha256: String,
}

pub struct Recovery {
    pub directory: PathBuf,
    pub report: Value,
    pub text: Option<String>,
}

fn sha(raw: &[u8]) -> String {
    format!("{:x}", Sha256::digest(raw))
}

fn name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 100
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c))
}

fn path_segment(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c))
}

/// Transform only a supported exact URL with an explicit, single-segment ref.
/// Source membership is the caller's responsibility; titles and text are never searched.
pub fn retrieval_url(source: &PrimarySource, git_ref: &str, git_path: &str) -> Result<String> {
    let original = &source.original_url;
    ensure!(
        !original.is_empty()
            && original.len() <= 2048
            && !original
                .chars()
                .any(|c| c.is_control() || c.is_whitespace())
            && !original.contains(['\\', '%', '?']),
        "unsupported_url_characters"
    );
    // Inspect the literal authority before URL parsing normalizes default ports or path dots.
    let tail = original
        .strip_prefix("https://")
        .context("https_required")?;
    let (authority, rest) = tail.split_once('/').context("url_path_required")?;
    let host = authority.to_ascii_lowercase();
    ensure!(
        host == "github.com" || host == HOST,
        "unsupported_url_origin"
    );
    ensure!(name(git_ref), "unsupported_git_ref");
    ensure!(
        git_path.split('/').all(path_segment),
        "unsupported_git_path"
    );
    ensure!(
        git_path.to_ascii_lowercase().ends_with(".md")
            || git_path.to_ascii_lowercase().ends_with(".markdown"),
        "markdown_path_required"
    );
    let literal_path = rest.split('#').next().unwrap_or_default();
    let parts: Vec<_> = literal_path.split('/').collect();
    ensure!(parts.len() >= 4, "unsupported_github_form");
    let (owner, repository) = (parts[0], parts[1]);
    ensure!(
        name(owner) && name(repository) && !repository.ends_with(".git"),
        "unsupported_repository_name"
    );
    let expected = format!(
        "{owner}/{repository}/{}{git_ref}/{git_path}",
        if host == "github.com" { "blob/" } else { "" }
    );
    ensure!(literal_path == expected, "explicit_ref_or_path_mismatch");
    let parsed = Url::parse(original).context("invalid_url")?;
    ensure!(parsed.scheme() == "https", "https_required");
    Ok(format!(
        "https://{HOST}/{owner}/{repository}/{git_ref}/{git_path}"
    ))
}

// Conservative public-unicast allowlist. Special-purpose ranges fail closed.
// This is not a claim that an address belongs to GitHub; TLS verifies the hostname.
fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !(a == 0
                || a == 10
                || a == 127
                || a >= 224
                || (a == 100 && (64..=127).contains(&b))
                || (a == 169 && b == 254)
                || (a == 172 && (16..=31).contains(&b))
                || (a == 192
                    && (b == 168 || (b == 0 && (c == 0 || c == 2)) || (b == 88 && c == 99)))
                || (a == 198 && (b == 18 || b == 19 || (b == 51 && c == 100)))
                || (a == 203 && b == 0 && c == 113))
        }
        IpAddr::V6(ip) => {
            let s = ip.segments();
            (s[0] & 0xe000) == 0x2000
                && !(s[0] == 0x2001 && (s[1] < 0x0200 || s[1] == 0x0db8))
                && s[0] != 0x2002
                && !(s[0] == 0x3fff && s[1] < 0x1000)
        }
    }
}

fn validate_addresses(mut addresses: Vec<SocketAddr>) -> Result<Vec<SocketAddr>> {
    ensure!(!addresses.is_empty(), "dns_empty");
    ensure!(
        addresses.iter().all(|a| {
            a.port() == 443
                && public_ip(a.ip())
                && match a {
                    SocketAddr::V6(v6) => v6.scope_id() == 0,
                    SocketAddr::V4(_) => true,
                }
        }),
        "dns_contains_unapproved_address"
    );
    addresses.sort_unstable();
    addresses.dedup();
    Ok(addresses)
}

fn client(addresses: &[SocketAddr]) -> Result<Client> {
    Ok(Client::builder()
        .https_only(true)
        .no_proxy()
        .redirect(Policy::none())
        .retry(reqwest::retry::never())
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .no_zstd()
        .timeout(TIMEOUT)
        .resolve_to_addrs(HOST, addresses)
        .build()?)
}

struct Capture {
    file: File,
    raw: Vec<u8>,
    headers: HeaderMap,
    status: Option<u16>,
    eof: bool,
    report: Value,
    directory: PathBuf,
}

impl Capture {
    fn failure(&mut self, code: &str) {
        self.report["failures"]
            .as_array_mut()
            .unwrap()
            .push(json!(code));
    }

    fn persist(&mut self) -> Result<()> {
        self.report["raw_retained_bytes"] = json!(self.raw.len());
        self.report["retained_bytes"] = json!(self.raw.len());
        self.report["raw_retained_sha256"] = json!(sha(&self.raw));
        let path = self.directory.join("report.json");
        let temporary = self.directory.join("report.json.tmp");
        std::fs::write(&temporary, serde_json::to_vec_pretty(&self.report)?)?;
        std::fs::rename(temporary, path)?;
        Ok(())
    }

    fn retain(&mut self, chunk: &[u8]) -> Result<bool> {
        let count = chunk.len().min(MAX_BYTES - self.raw.len());
        // Synchronous writes avoid a canceled async write outliving the recovery future.
        self.file.write_all(&chunk[..count])?;
        self.raw.extend_from_slice(&chunk[..count]);
        self.report["observed_body_bytes"] =
            json!(self.report["observed_body_bytes"].as_u64().unwrap() + chunk.len() as u64);
        if self.raw.len() == MAX_BYTES {
            self.report["body_limit_reached"] = json!(true);
            self.failure("body_limit_reached_without_eof");
        }
        self.persist()?;
        Ok(self.raw.len() < MAX_BYTES)
    }

    fn receive_headers(&mut self, status: u16, headers: HeaderMap) -> Result<()> {
        self.status = Some(status);
        self.headers = headers;
        self.report["response_status"] = json!(status);
        self.report["response_headers"] = json!(self
            .headers
            .iter()
            .map(|(name, value)| { json!({"name":name.as_str(), "value_bytes":value.as_bytes()}) })
            .collect::<Vec<_>>());
        self.persist()
    }

    fn finish(&mut self) -> Result<Option<String>> {
        let mut header_error = false;
        for name in [
            "content-length",
            "content-type",
            "content-encoding",
            "location",
        ] {
            if self.headers.get_all(name).iter().count() > 1 {
                header_error = true;
            }
        }
        if header_error {
            self.failure("ambiguous_response_header");
        }
        let length_valid = match self.headers.get("content-length") {
            None => true,
            Some(value) => value.to_str().ok().is_some_and(|value| {
                !value.is_empty()
                    && value.bytes().all(|c| c.is_ascii_digit())
                    && (value == "0" || !value.starts_with('0'))
                    && value.parse::<usize>().ok() == Some(self.raw.len())
            }),
        };
        if self.eof && !length_valid {
            self.failure("content_length_invalid_or_mismatched");
        }
        let complete = self.eof && self.report["failures"].as_array().unwrap().is_empty();
        self.report["response_complete"] = json!(complete);
        if complete {
            self.report["complete_response_sha256"] = json!(sha(&self.raw));
            self.report["omitted_body_bytes"] = json!(0);
        }
        if self.status != Some(200) {
            self.failure(if self.status.is_some_and(|s| (300..400).contains(&s)) {
                "redirect_not_followed"
            } else {
                "http_status_not_200_or_missing"
            });
        }
        let content_type = self
            .headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        if content_type != "text/plain" && content_type != "text/markdown" {
            self.failure("unsupported_content_type");
        }
        if self.headers.get("content-encoding").is_some_and(|v| {
            v.to_str()
                .map_or(true, |v| !v.trim().eq_ignore_ascii_case("identity"))
        }) {
            self.failure("unsupported_content_encoding");
        }
        let decoded = String::from_utf8(self.raw.clone());
        let text = match decoded {
            Ok(text) => {
                let start = text
                    .trim_start_matches('\u{feff}')
                    .trim_start()
                    .to_ascii_lowercase();
                if start.starts_with("<html") || start.starts_with("<!doctype html") {
                    self.failure("html_is_not_markdown_body");
                }
                Some(text)
            }
            Err(_) => {
                self.failure("invalid_or_incomplete_utf8");
                None
            }
        };
        let document_complete = complete && self.report["failures"].as_array().unwrap().is_empty();
        self.report["document_complete"] = json!(document_complete);
        self.report["operation_finished"] = json!(true);
        self.report["status"] = json!(if document_complete {
            "complete"
        } else {
            "incomplete"
        });
        if document_complete {
            let identity = serde_json::to_vec(&json!({"source":self.report["source"],
                "retrieval_url":self.report["retrieval_url"],"sha256":sha(&self.raw),
                "synthetic":self.report["synthetic"]}))?;
            self.report["recovered_body_id"] = json!(format!("primary-body:{}", sha(&identity)));
        }
        self.persist()?;
        Ok(if document_complete { text } else { None })
    }
}

async fn receive(capture: &mut Capture, client: &Client, url: &str) -> Result<()> {
    capture.report["requests_attempted"] = json!(1);
    capture.persist()?;
    let response = client
        .get(url)
        .header("Accept", "text/plain, text/markdown")
        .header("Accept-Encoding", "identity")
        .send()
        .await
        .map_err(|_| anyhow::anyhow!("http_request_failed"))?;
    ensure!(response.url().as_str() == url, "unexpected_response_url");
    capture.receive_headers(response.status().as_u16(), response.headers().clone())?;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| anyhow::anyhow!("response_stream_failed"))?;
        if !capture.retain(&chunk)? {
            return Ok(());
        }
    }
    capture.eof = true;
    Ok(())
}

/// Recover one exact primary body. Unsupported inputs return a persisted rejected report.
/// Artifact errors return Err; network and document failures return Recovery with text=None.
pub async fn recover(
    source: &PrimarySource,
    git_ref: &str,
    git_path: &str,
    directory: &Path,
    fixture: bool,
) -> Result<Recovery> {
    std::fs::create_dir(directory).context("recovery_directory_must_be_new")?;
    let started = Instant::now();
    let report = json!({"schema":"primary-body-recovery-v1","source":source,
        "original_url":source.original_url,"retrieval_url":null,"status":"pending","operation_finished":false,
        "fixture":fixture,"synthetic":fixture,"synthetic_scope":if fixture {Some("synthetic full body; not remote source content")} else {None},
        "git_ref":git_ref,"git_path":git_path,"resolved_commit":null,
        "ref_binding":"explicit_caller_assertion","requests_attempted":0,
        "response_status":null,"response_headers":[],"response_complete":false,
        "document_complete":false,"retained_bytes":0,"raw_retained_bytes":0,"raw_retained_sha256":sha(&[]),
        "complete_response_sha256":null,"recovered_body_id":null,"omitted_body_bytes":null,
        "observed_body_bytes":0,"body_limit_reached":false,"raw_artifact":"raw.bin",
        "max_body_bytes":MAX_BYTES,"timeout_seconds":20,"failures":[],
        "started_unix_ms":SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
        "dns_addresses":[],"dns_policy":"all public results validated and pinned; no DNSSEC claim",
        "header_scope":"parsed header value bytes; not wire headers",
        "byte_scope":"retained entity bytes; transport framing and socket buffers excluded",
        "jev_calls":0,"model_calls":0});
    let mut capture = Capture {
        file: OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(directory.join("raw.bin"))?,
        raw: Vec::new(),
        headers: HeaderMap::new(),
        status: None,
        eof: false,
        report,
        directory: directory.to_path_buf(),
    };
    capture.persist()?;
    let url = match retrieval_url(source, git_ref, git_path) {
        Ok(url) => url,
        Err(error) => {
            capture.failure(&error.to_string());
            capture.report["status"] = json!("rejected");
            capture.report["operation_finished"] = json!(true);
            capture.persist()?;
            return Ok(Recovery {
                directory: directory.to_path_buf(),
                report: capture.report,
                text: None,
            });
        }
    };
    capture.report["retrieval_url"] = json!(url);
    capture.report["status"] = json!("incomplete");
    capture.persist()?;
    if fixture {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "text/markdown; charset=utf-8".parse()?);
        headers.insert("content-length", FIXTURE.len().to_string().parse()?);
        capture.receive_headers(200, headers)?;
        capture.retain(FIXTURE)?;
        capture.eof = true;
    } else {
        let operation = async {
            let addresses = tokio::net::lookup_host((HOST, 443))
                .await
                .map_err(|_| anyhow::anyhow!("dns_failed"))?
                .collect();
            let addresses = validate_addresses(addresses)?;
            capture.report["dns_addresses"] = json!(addresses);
            capture.persist()?;
            let client = client(&addresses).context("http_client_failed")?;
            receive(&mut capture, &client, &url).await
        };
        match tokio::time::timeout(TIMEOUT, operation).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                // A partial artifact write must not become a normal network failure result.
                if error.downcast_ref::<std::io::Error>().is_some() {
                    return Err(error);
                }
                capture.failure(&error.to_string());
            }
            Err(_) => capture.failure("timeout"),
        }
    }
    capture.report["elapsed_ms"] = json!(started.elapsed().as_millis());
    let text = capture.finish()?;
    Ok(Recovery {
        directory: directory.to_path_buf(),
        report: capture.report,
        text,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    fn source() -> PrimarySource {
        PrimarySource {
            document_id: "doc-one".into(),
            source_id: "saved-source".into(),
            title: "Original title".into(),
            original_url:
                "https://github.com/stellar/stellar-protocol/blob/master/ecosystem/sep-0024.md"
                    .into(),
            text_sha256: sha(b"original excerpt"),
        }
    }

    fn capture(directory: &Path, status: u16, headers: &[(&str, &str)]) -> Capture {
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            map.append(
                reqwest::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        Capture {
            file: File::create(directory.join("raw.bin")).unwrap(),
            raw: vec![],
            headers: map,
            status: Some(status),
            eof: false,
            report: json!({"failures":[],"observed_body_bytes":0,"response_complete":false,"document_complete":false,
                "complete_response_sha256":null,"omitted_body_bytes":null,"source":source(),"synthetic":false,
                "retrieval_url":"https://raw.githubusercontent.com/stellar/stellar-protocol/master/ecosystem/sep-0024.md"}),
            directory: directory.into(),
        }
    }

    #[test]
    fn exact_forms_and_fragment_preserve_identity() {
        let mut source = source();
        source.original_url.push_str("#status");
        let expected = "https://raw.githubusercontent.com/stellar/stellar-protocol/master/ecosystem/sep-0024.md";
        assert_eq!(
            retrieval_url(&source, "master", "ecosystem/sep-0024.md").unwrap(),
            expected
        );
        source.original_url = expected.into();
        assert_eq!(
            retrieval_url(&source, "master", "ecosystem/sep-0024.md").unwrap(),
            expected
        );
    }

    #[test]
    fn dangerous_and_unsupported_urls_are_rejected() {
        for original in [
            "http://github.com/o/r/blob/main/a.md",
            "file:///a.md",
            "https://127.0.0.1/o/r/blob/main/a.md",
            "https://github.com.evil.test/o/r/blob/main/a.md",
            "https://user@github.com/o/r/blob/main/a.md",
            "https://github.com:443/o/r/blob/main/a.md",
            "https://github.com/o/r/blob/main/a.md?x=1",
            "https://github.com/o/r/blob/main/a%2emd",
            "https://github.com/o/r/tree/main/a.md",
            "https://github.com/o/r/raw/main/a.md",
            "https://github.com/o/r/blob/main/../a.md",
            "https://github.com/o/r/blob/main/a.md\n",
            "https://github.com/o/r/blob/main/a\\b.md",
            "https://raw.githubusercontent.com/o/r/refs/heads/main/a.md",
            "https://github.com/o/r.git/blob/main/a.md",
            "https://github.com/o/r/blob/main//a.md",
        ] {
            let mut s = source();
            s.original_url = original.into();
            assert!(retrieval_url(&s, "main", "a.md").is_err(), "{original}");
        }
    }

    #[test]
    fn refs_paths_and_metadata_never_supply_guesses() {
        let mut s = source();
        for (git_ref, path) in [
            ("master/feature", "ecosystem/sep-0024.md"),
            ("..", "ecosystem/sep-0024.md"),
            ("master", "../ecosystem/sep-0024.md"),
            ("master", "ecosystem/sep-0024.html"),
            ("master", "ecosystem/SEP-0024.md"),
            ("main", "ecosystem/sep-0024.md"),
        ] {
            assert!(retrieval_url(&s, git_ref, path).is_err());
        }
        s.title = "https://github.com/o/r/blob/main/a.md".into();
        s.original_url = "https://example.com/a.md".into();
        assert!(retrieval_url(&s, "main", "a.md").is_err());
    }

    #[test]
    fn public_dns_policy_rejects_any_unsafe_result() {
        for ip in [
            "0.0.0.1",
            "10.1.1.1",
            "100.64.1.1",
            "127.0.0.1",
            "169.254.1.1",
            "172.31.0.1",
            "192.0.0.9",
            "192.0.2.1",
            "192.168.0.1",
            "192.88.99.1",
            "198.18.0.1",
            "198.51.100.1",
            "203.0.113.1",
            "224.1.1.1",
            "255.255.255.255",
            "::1",
            "::ffff:127.0.0.1",
            "fc00::1",
            "fe80::1",
            "2001:db8::1",
            "2002:0808:0808::1",
            "3fff::1",
            "64:ff9b::808:808",
        ] {
            let address = SocketAddr::new(ip.parse().unwrap(), 443);
            assert!(
                validate_addresses(vec!["8.8.8.8:443".parse().unwrap(), address]).is_err(),
                "{ip}"
            );
        }
        assert!(validate_addresses(vec![]).is_err());
        assert!(validate_addresses(vec!["8.8.8.8:80".parse().unwrap()]).is_err());
        assert!(validate_addresses(vec![
            "8.8.8.8:443".parse().unwrap(),
            "[2606:4700:4700::1111]:443".parse().unwrap()
        ])
        .is_ok());
    }

    #[tokio::test]
    async fn fixture_is_synthetic_deterministic_and_zero_request() {
        let root = tempfile::tempdir().unwrap();
        let first = recover(
            &source(),
            "master",
            "ecosystem/sep-0024.md",
            &root.path().join("a"),
            true,
        )
        .await
        .unwrap();
        let second = recover(
            &source(),
            "master",
            "ecosystem/sep-0024.md",
            &root.path().join("b"),
            true,
        )
        .await
        .unwrap();
        assert_eq!(first.text, second.text);
        assert_eq!(
            first.report["complete_response_sha256"],
            second.report["complete_response_sha256"]
        );
        assert_eq!(first.report["requests_attempted"], 0);
        assert_eq!(first.report["dns_addresses"], json!([]));
        assert_eq!(first.report["fixture"], true);
        assert_eq!(first.report["synthetic"], true);
        assert_eq!(first.report["document_complete"], true);
        assert_eq!(first.report["source"]["text_sha256"], source().text_sha256);
        assert_eq!(
            std::fs::read(first.directory.join("raw.bin")).unwrap(),
            FIXTURE
        );
        assert!(recover(
            &source(),
            "master",
            "ecosystem/sep-0024.md",
            &first.directory,
            true
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn invalid_request_has_saved_binding_and_no_fetch() {
        let root = tempfile::tempdir().unwrap();
        let outcome = recover(
            &source(),
            "wrong",
            "ecosystem/sep-0024.md",
            &root.path().join("a"),
            false,
        )
        .await
        .unwrap();
        assert!(outcome.text.is_none());
        assert_eq!(outcome.report["status"], "rejected");
        assert_eq!(outcome.report["requests_attempted"], 0);
        assert_eq!(outcome.report["source"]["document_id"], "doc-one");
        assert_eq!(
            std::fs::read(outcome.directory.join("raw.bin")).unwrap(),
            b""
        );
    }

    #[test]
    fn complete_unicode_preserves_exact_bytes() {
        let root = tempfile::tempdir().unwrap();
        let mut state = capture(
            root.path(),
            200,
            &[("content-type", "text/markdown; charset=utf-8")],
        );
        let bytes = "# Heading\n\nWallet 🌟\n".as_bytes();
        state.retain(&bytes[..15]).unwrap();
        state.retain(&bytes[15..]).unwrap();
        state.eof = true;
        assert_eq!(state.finish().unwrap().unwrap().as_bytes(), bytes);
        assert_eq!(state.report["response_complete"], true);
        assert_eq!(state.report["complete_response_sha256"], sha(bytes));
    }

    #[test]
    fn unsuitable_bodies_preserve_raw_without_document_text() {
        for (status, headers, body, eof) in [
            (
                404,
                vec![("content-type", "text/plain")],
                b"missing".as_slice(),
                true,
            ),
            (
                302,
                vec![
                    ("content-type", "text/plain"),
                    ("location", "http://127.0.0.1/"),
                ],
                b"moved".as_slice(),
                true,
            ),
            (
                200,
                vec![("content-type", "text/html")],
                b"<html>error".as_slice(),
                true,
            ),
            (
                200,
                vec![("content-type", "text/plain")],
                b" \n<!DOCTYPE html>".as_slice(),
                true,
            ),
            (
                200,
                vec![("content-type", "text/plain"), ("content-encoding", "gzip")],
                b"abc".as_slice(),
                true,
            ),
            (
                200,
                vec![("content-type", "text/plain"), ("content-length", "9")],
                b"abc".as_slice(),
                true,
            ),
            (
                200,
                vec![("content-type", "text/plain"), ("content-length", "03")],
                b"abc".as_slice(),
                true,
            ),
            (
                200,
                vec![
                    ("content-type", "text/plain"),
                    ("content-length", "3"),
                    ("content-length", "3"),
                ],
                b"abc".as_slice(),
                true,
            ),
            (
                200,
                vec![("content-type", "text/plain")],
                b"\xf0\x9f".as_slice(),
                false,
            ),
            (
                200,
                vec![("content-type", "text/plain")],
                b"\xff".as_slice(),
                true,
            ),
            (
                200,
                vec![("content-type", "text/plain")],
                b"prefix".as_slice(),
                false,
            ),
        ] {
            let root = tempfile::tempdir().unwrap();
            let mut state = capture(root.path(), status, &headers);
            state.retain(body).unwrap();
            state.eof = eof;
            if !eof {
                state.failure("stream_failed");
            }
            assert!(state.finish().unwrap().is_none(), "{status} {headers:?}");
            assert_eq!(state.report["document_complete"], false);
            assert_eq!(std::fs::read(root.path().join("raw.bin")).unwrap(), body);
        }
    }

    #[test]
    fn exact_cap_and_oversize_remain_incomplete_with_bounded_prefix() {
        for extra in [0, 8] {
            let root = tempfile::tempdir().unwrap();
            let mut state = capture(root.path(), 200, &[("content-type", "text/plain")]);
            assert!(!state.retain(&vec![b'a'; MAX_BYTES + extra]).unwrap());
            assert!(state.finish().unwrap().is_none());
            assert_eq!(state.raw.len(), MAX_BYTES);
            assert_eq!(state.report["response_complete"], false);
            assert_eq!(state.report["complete_response_sha256"], Value::Null);
            assert_eq!(state.report["omitted_body_bytes"], Value::Null);
        }
    }

    async fn serve(reply: &'static [u8], delay: bool) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            let count = stream.read(&mut request).await.unwrap();
            assert!(count > 0);
            stream.write_all(reply).await.unwrap();
            stream.flush().await.unwrap();
            if delay {
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
        (format!("http://{address}/"), task)
    }

    #[tokio::test]
    async fn broken_transport_preserves_prefix_before_failure() {
        let root = tempfile::tempdir().unwrap();
        let mut state = capture(root.path(), 200, &[]);
        let (url, server) = serve(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 9\r\n\r\nabc",
            false,
        )
        .await;
        let client = Client::builder()
            .no_proxy()
            .redirect(Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .unwrap();
        let result = receive(&mut state, &client, &url).await;
        assert!(result.is_err());
        state.failure("response_stream_failed");
        assert!(state.finish().unwrap().is_none());
        assert_eq!(state.raw, b"abc");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn timeout_preserves_prefix_and_redirect_never_follows() {
        let root = tempfile::tempdir().unwrap();
        let mut state = capture(root.path(), 200, &[]);
        let (url, server) = serve(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 9\r\n\r\nabc",
            true,
        )
        .await;
        let client = Client::builder()
            .no_proxy()
            .redirect(Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .unwrap();
        assert!(tokio::time::timeout(
            Duration::from_millis(100),
            receive(&mut state, &client, &url)
        )
        .await
        .is_err());
        state.failure("timeout");
        assert!(state.finish().unwrap().is_none());
        assert_eq!(state.raw, b"abc");
        server.abort();
        let (url,server) = serve(b"HTTP/1.1 302 Found\r\nContent-Type: text/plain\r\nLocation: http://127.0.0.1:1/\r\nContent-Length: 4\r\n\r\nmove",false).await;
        let other = tempfile::tempdir().unwrap();
        let mut state = capture(other.path(), 200, &[]);
        receive(&mut state, &client, &url).await.unwrap();
        assert!(state.finish().unwrap().is_none());
        assert_eq!(state.report["response_complete"], true);
        assert_eq!(state.raw, b"move");
        server.await.unwrap();
    }
}
