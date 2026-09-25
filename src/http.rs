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
    hosts: Arc<HostLimits>,
    shared: Arc<SharedGets>,
    /// Host-wide rate-limit gates for source requests. Jev requests keep their own provider
    /// policy, so the Jev clone has none.
    gates: Option<Arc<crate::governor::Governor>>,
    /// Requests this run booked in advance per source scope, with the window they were booked in.
    /// They are not counted again while that window lasts.
    prepaid: Arc<Mutex<std::collections::HashMap<String, (u64, u64)>>>,
    load: Arc<LoadCounters>,
    allow_loopback: bool,
    limits: Option<Arc<RequestLimits>>,
    /// Set on a public reader clone: see `public_reader`.
    public: Option<PublicPolicy>,
}

/// The largest body a public read keeps.
pub const PUBLIC_BODY_LIMIT: usize = 2 * 1024 * 1024;
const PUBLIC_TIMEOUT: Duration = Duration::from_secs(10);
const PUBLIC_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const PUBLIC_TYPES: &[&str] = &["text/html", "text/markdown", "text/plain"];
/// The only request headers a public read sends.
const PUBLIC_HEADERS: &[(&str, &str)] = &[
    ("accept", "text/html, text/markdown;q=0.9, text/plain;q=0.8"),
    ("accept-encoding", "identity"),
    (
        "user-agent",
        concat!("stellar-raven-jev/", env!("CARGO_PKG_VERSION")),
    ),
];

/// A public read that the transport rules refused: a host that is not a public name, a DNS answer
/// or connected peer that is not public, a redirect, an encoded or unsupported body, or a body
/// past the limit.
#[derive(Debug)]
pub struct Refused(pub String);

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Public read refused: {}", self.0)
    }
}

impl std::error::Error for Refused {}

/// True for a public unicast destination. IPv4-mapped IPv6 addresses are judged as IPv4.
pub fn public_address(ip: std::net::IpAddr) -> bool {
    use std::net::IpAddr;
    let ip = match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(ip, IpAddr::V4),
        v4 => v4,
    };
    match ip {
        IpAddr::V4(v4) => {
            let [a, b, c, _] = v4.octets();
            !(v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_multicast()
                || v4.is_broadcast()
                || v4.is_documentation()
                || a == 0
                || a >= 240
                || (a == 100 && (64..128).contains(&b))
                || (a == 198 && (b == 18 || b == 19))
                || (a == 192 && b == 0 && c == 0))
        }
        IpAddr::V6(v6) => {
            let s = v6.segments();
            // NAT64 (64:ff9b::/96) carries an IPv4 destination in its low 32 bits.
            if s[..6] == [0x64, 0xff9b, 0, 0, 0, 0] {
                let [.., hi, lo] = s;
                return public_address(IpAddr::V4(std::net::Ipv4Addr::from(
                    (u32::from(hi) << 16) | u32::from(lo),
                )));
            }
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || s[..6] == [0; 6]
                || (s[0] & 0xfe00) == 0xfc00
                || (s[0] & 0xffc0) == 0xfe80
                || (s[0] & 0xffc0) == 0xfec0
                || (s[0] == 0x2001 && s[1] == 0x0db8)
                || (s[0] & 0xfff0) == 0x3ff0)
        }
    }
}

/// Test-only name resolution for a public reader: fixed answers, loopback allowed, and plain
/// HTTP on a local port in place of HTTPS on 443.
#[cfg(test)]
#[derive(Clone, Default)]
pub(crate) struct TestDns {
    pub answers: std::collections::HashMap<String, Vec<std::net::IpAddr>>,
    pub loopback: bool,
    pub port: Option<u16>,
    pub timeout: Option<Duration>,
}

#[derive(Clone, Default)]
struct PublicPolicy {
    #[cfg(test)]
    test: Option<Arc<TestDns>>,
}

impl PublicPolicy {
    fn allows(&self, ip: std::net::IpAddr) -> bool {
        #[cfg(test)]
        if self.test.as_ref().is_some_and(|t| t.loopback) {
            let ip = match ip {
                std::net::IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(ip, std::net::IpAddr::V4),
                v4 => v4,
            };
            if ip.is_loopback() {
                return true;
            }
        }
        public_address(ip)
    }
}

/// A DNS answer that contained an address the public reader must not connect to.
#[derive(Debug)]
struct NonPublicAnswer(String);

impl std::fmt::Display for NonPublicAnswer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the DNS answer for {} is not only public addresses",
            self.0
        )
    }
}

impl std::error::Error for NonPublicAnswer {}

/// Resolves through the system resolver and returns the answer only when every address is public,
/// so the client connects only to checked addresses and keeps the host name for TLS.
struct PublicResolver(PublicPolicy);

impl reqwest::dns::Resolve for PublicResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let policy = self.0.clone();
        let host = name.as_str().to_owned();
        Box::pin(async move {
            #[cfg(test)]
            let fixed = policy
                .test
                .as_ref()
                .and_then(|t| t.answers.get(&host).cloned());
            #[cfg(not(test))]
            let fixed: Option<Vec<std::net::IpAddr>> = None;
            let addresses: Vec<std::net::SocketAddr> = match fixed {
                Some(ips) => ips
                    .into_iter()
                    .map(|ip| std::net::SocketAddr::new(ip, 0))
                    .collect(),
                None => tokio::net::lookup_host((host.as_str(), 0)).await?.collect(),
            };
            if addresses.is_empty() || !addresses.iter().all(|a| policy.allows(a.ip())) {
                return Err(Box::new(NonPublicAnswer(host)) as Box<_>);
            }
            Ok(Box::new(addresses.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

/// HTTPS only, checked DNS, no redirects, no proxy, no pooled connections (each request resolves
/// and is checked again), and a total time limit that covers the body.
fn public_client(policy: &PublicPolicy) -> Result<Client> {
    #[cfg(test)]
    let (plain, timeout) = policy.test.as_ref().map_or((false, PUBLIC_TIMEOUT), |t| {
        (t.port.is_some(), t.timeout.unwrap_or(PUBLIC_TIMEOUT))
    });
    #[cfg(not(test))]
    let (plain, timeout) = (false, PUBLIC_TIMEOUT);
    Ok(Client::builder()
        .dns_resolver(Arc::new(PublicResolver(policy.clone())))
        .https_only(!plain)
        .redirect(Policy::none())
        .no_proxy()
        .referer(false)
        .pool_max_idle_per_host(0)
        .connect_timeout(PUBLIC_CONNECT_TIMEOUT)
        .timeout(timeout)
        .build()?)
}

/// An error and its causes on one line.
fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}

/// A media type from a Content-Type value, without parameters.
fn media_type(value: &str) -> String {
    value
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

/// In-flight requests per host. One slow host cannot hold the permits that other hosts need.
struct HostLimits {
    per_host: usize,
    semaphores: Mutex<std::collections::HashMap<String, Arc<Semaphore>>>,
}

impl HostLimits {
    fn new(per_host: usize) -> Arc<Self> {
        Arc::new(Self {
            per_host: per_host.max(1),
            semaphores: Mutex::default(),
        })
    }

    fn semaphore(&self, host: &str) -> Arc<Semaphore> {
        self.semaphores
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(host.to_owned())
            .or_insert_with(|| Arc::new(Semaphore::new(self.per_host)))
            .clone()
    }
}

/// A source request waits at a closed rate-limit gate only this long; past it, the request fails
/// at once and is counted, so a closed gate never stalls the whole fetch stage.
const GATE_WAIT_LIMIT: Duration = Duration::from_secs(4);
/// Gate closure after a 429 without a numeric Retry-After, and the longest closure any signal
/// can set.
const GATE_DEFAULT_CLOSE: Duration = Duration::from_secs(10);
const GATE_MAX_CLOSE: Duration = Duration::from_secs(600);
/// The latest reset a source can advertise for a request window.
const WINDOW_MAX_RESET: Duration = Duration::from_secs(86_400);
/// How long a question waits for room in a source's advertised window before it sends without a
/// booking: one full window of a one-minute source.
const BOOKING_WAIT_LIMIT: Duration = Duration::from_secs(65);

/// Source requests refused by rate limits, source server errors, and gate waits in this run.
#[derive(Default)]
struct LoadCounters {
    rate_limited: AtomicU64,
    server_errors: AtomicU64,
    gate_wait_ms: AtomicU64,
    booking_wait_ms: AtomicU64,
    /// Time spent waiting for fetch slots on capped source hosts.
    slot_wait_ms: AtomicU64,
    /// Source requests sent, per host.
    sent: Mutex<std::collections::BTreeMap<String, u64>>,
    /// Time from send to the last body byte of every completed source response, per host.
    latency_ms: Mutex<std::collections::BTreeMap<String, Vec<u64>>>,
    /// Source requests in flight now, and the most at once, per host.
    in_flight: Mutex<std::collections::BTreeMap<String, (u64, u64)>>,
}

/// One source request in flight; dropping it, also by cancellation, ends it.
struct InFlight<'a> {
    load: &'a LoadCounters,
    host: String,
}

impl<'a> InFlight<'a> {
    fn start(load: &'a LoadCounters, host: &str) -> Self {
        let mut hosts = load.in_flight.lock().unwrap_or_else(|e| e.into_inner());
        let entry = hosts.entry(host.to_owned()).or_default();
        entry.0 += 1;
        entry.1 = entry.1.max(entry.0);
        Self {
            load,
            host: host.to_owned(),
        }
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        let mut hosts = self
            .load
            .in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = hosts.get_mut(&self.host) {
            entry.0 = entry.0.saturating_sub(1);
        }
    }
}

/// A source request that was not sent because the source's rate-limit gate is closed.
#[derive(Debug)]
pub struct SourceRateLimited {
    pub scope: String,
    pub wait: Duration,
}

impl std::fmt::Display for SourceRateLimited {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Source rate limit: {} is closed for {} ms more; the request was not sent",
            self.scope,
            self.wait.as_millis()
        )
    }
}

impl std::error::Error for SourceRateLimited {}

/// An HTTP-date in the preferred form (`Wed, 21 Oct 2026 07:28:00 GMT`), as Unix seconds.
fn http_date_seconds(text: &str) -> Option<i64> {
    let parts: Vec<&str> = text.split_whitespace().collect();
    let [_, day, month, year, time, "GMT"] = parts[..] else {
        return None;
    };
    let month = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]
    .iter()
    .position(|m| *m == month)? as i64
        + 1;
    let days = crate::rank::civil_days(year.parse().ok()?, month, day.parse().ok()?)?;
    let clock: Vec<i64> = time
        .split(':')
        .map(|p| p.parse().ok())
        .collect::<Option<_>>()?;
    let [h, m, s] = clock[..] else {
        return None;
    };
    ((0..24).contains(&h) && (0..60).contains(&m) && (0..61).contains(&s))
        .then_some(days * 86_400 + h * 3_600 + m * 60 + s)
}

/// The rate-limit signals in a source response. Retry-After is seconds or an HTTP-date. A reset
/// above 10^9 is a Unix time in seconds; a smaller one is seconds from now. Other values are
/// ignored, and every duration is capped before conversion, so no header can overflow one.
fn source_signals(
    status: u16,
    headers: &std::collections::BTreeMap<String, String>,
) -> crate::governor::SourceSignals {
    let number = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
    };
    let capped = |value: f64, cap: Duration| Duration::from_secs_f64(value.min(cap.as_secs_f64()));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let retry_after = number("retry-after").or_else(|| {
        let at = http_date_seconds(headers.get("retry-after")?.trim())?;
        Some((at as f64 - now.as_secs_f64()).max(0.0))
    });
    crate::governor::SourceSignals {
        status,
        retry_after: retry_after.map(|s| capped(s, GATE_MAX_CLOSE)),
        limit: number("x-ratelimit-limit")
            .filter(|v| *v <= u32::MAX as f64)
            .map(|v| v as u64),
        remaining: number("x-ratelimit-remaining")
            .filter(|v| *v <= u32::MAX as f64)
            .map(|v| v as u64),
        reset_ms: number("x-ratelimit-reset").map(|reset| {
            let until = if reset > 1_000_000_000.0 {
                capped((reset - now.as_secs_f64()).max(0.0), WINDOW_MAX_RESET)
            } else {
                capped(reset, WINDOW_MAX_RESET)
            };
            (now + until).as_millis() as u64
        }),
    }
}

/// One response per identical GET within a run, keyed by URL and a digest of the headers.
type SharedGets = Mutex<
    std::collections::HashMap<String, Arc<tokio::sync::OnceCell<Result<HttpResponse, String>>>>,
>;

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

    /// A clone with its own per-host request limit that shares the run folder and the raw-file
    /// sequence.
    pub fn with_concurrency(&self, concurrency: usize) -> Self {
        let mut clone = self.clone();
        clone.hosts = HostLimits::new(concurrency);
        clone.gates = None;
        clone
    }

    /// A clone whose requests honor, and report, source rate-limit signals shared through
    /// `governor`.
    pub fn with_source_gates(&self, governor: Arc<crate::governor::Governor>) -> Self {
        let mut clone = self.clone();
        clone.gates = Some(governor);
        clone
    }

    /// Source load in this run: requests refused by rate limits (429 responses and requests not
    /// sent at a closed gate), server errors (5xx), and time spent waiting at gates.
    pub fn load_summary(&self) -> Value {
        json!({
            "source_rate_limited_requests": self.load.rate_limited.load(Ordering::Relaxed),
            "source_server_errors": self.load.server_errors.load(Ordering::Relaxed),
            "source_gate_wait_ms": self.load.gate_wait_ms.load(Ordering::Relaxed),
                        "source_booking_wait_ms": self.load.booking_wait_ms.load(Ordering::Relaxed),
            "source_slot_wait_ms": self.load.slot_wait_ms.load(Ordering::Relaxed),
            "source_requests": *self.load.sent.lock().unwrap_or_else(|e| e.into_inner()),
            "source_latency": self.latency_summary(),
        })
    }

    /// Per host: completed responses and their send-to-last-byte time (p50, p95, max), requests
    /// sent that did not complete (failed or cut), and the most requests in flight at once.
    fn latency_summary(&self) -> Value {
        let sent = self
            .load
            .sent
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let latency = self
            .load
            .latency_ms
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let in_flight = self
            .load
            .in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        sent.iter()
            .map(|(host, count)| {
                let mut samples = latency.get(host).cloned().unwrap_or_default();
                samples.sort_unstable();
                let percentile = |p: f64| {
                    (!samples.is_empty())
                        .then(|| samples[((samples.len() - 1) as f64 * p).round() as usize])
                };
                (
                    host.clone(),
                    json!({
                        "completed": samples.len(),
                        "not_completed": count.saturating_sub(samples.len() as u64),
                        "p50_ms": percentile(0.5),
                        "p95_ms": percentile(0.95),
                        "max_ms": samples.last(),
                        "peak_in_flight": in_flight.get(host).map_or(0, |entry| entry.1),
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>()
            .into()
    }

    /// Hold one fetch slot on every capped host in `caps` (host, slots), all at once, waiting up
    /// to `wait`. `None` means no room in time and nothing is held. Without host state, or with
    /// no caps, nothing needs holding.
    pub async fn hold_hosts(
        &self,
        caps: &[(String, usize)],
        wait: Duration,
    ) -> Result<Option<crate::governor::HostHold>> {
        match &self.gates {
            Some(governor) if !caps.is_empty() => {
                let started = Instant::now();
                let hold = governor.hold_hosts(caps, wait).await?;
                self.load
                    .slot_wait_ms
                    .fetch_add(started.elapsed().as_millis() as u64, Ordering::Relaxed);
                Ok(hold)
            }
            _ => Ok(Some(crate::governor::HostHold::default())),
        }
    }

    /// Book this question's requests in every source scope's advertised window before any is
    /// sent, all at once, waiting up to about one window for room. `Some(wait)` means there was
    /// no room in time and nothing is booked: the caller should report busy and retry after
    /// `wait`. Scopes that advertise no window need no booking.
    pub async fn book_windows(&self, demands: &[(String, u64)]) -> Result<Option<Duration>> {
        self.book_windows_within(demands, BOOKING_WAIT_LIMIT).await
    }

    async fn book_windows_within(
        &self,
        demands: &[(String, u64)],
        limit: Duration,
    ) -> Result<Option<Duration>> {
        let Some(gates) = &self.gates else {
            return Ok(None);
        };
        let started = Instant::now();
        loop {
            match gates.book_windows(demands)? {
                crate::governor::Booking::Booked(booked) => {
                    let mut prepaid = self.prepaid.lock().unwrap_or_else(|e| e.into_inner());
                    for entry in booked {
                        prepaid.insert(entry.scope, (entry.count, entry.window_epoch));
                    }
                    return Ok(None);
                }
                crate::governor::Booking::Wait(wait) => {
                    let remaining = limit.saturating_sub(started.elapsed());
                    if remaining < wait {
                        return Ok(Some(wait));
                    }
                    self.load
                        .booking_wait_ms
                        .fetch_add(wait.as_millis() as u64, Ordering::Relaxed);
                    tokio::time::sleep(wait).await;
                }
            }
        }
    }

    /// Spend one booked request for `scope`, returning the window it was booked in.
    fn take_prepaid(&self, scope: &str) -> Option<u64> {
        let mut prepaid = self.prepaid.lock().unwrap_or_else(|e| e.into_inner());
        match prepaid.get_mut(scope) {
            Some((left, reset)) if *left > 0 => {
                *left -= 1;
                Some(*reset)
            }
            _ => None,
        }
    }

    pub fn run_dir(&self) -> &Path {
        &self.root
    }

    /// A recorder for a later call in an existing run folder: raw file numbers continue after the
    /// ones already there.
    pub fn resume(run_dir: &Path, config: &RunConfig) -> Result<Self> {
        let recorder = Self::new(run_dir, config)?;
        let next = std::fs::read_dir(run_dir.join("raw"))
            .into_iter()
            .flatten()
            .filter_map(|entry| {
                let name = entry.ok()?.file_name();
                name.to_str()?.split('.').next()?.parse::<u64>().ok()
            })
            .max()
            .map_or(0, |n| n + 1);
        recorder.sequence.store(next, Ordering::Relaxed);
        Ok(recorder)
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
            hosts: HostLimits::new(config.concurrency),
            shared: Arc::default(),
            gates: None,
            prepaid: Arc::default(),
            load: Arc::default(),
            allow_loopback: false,
            limits: None,
            public: None,
        })
    }

    /// A clone that reads public pages named by source rows: only GET with fixed headers, only
    /// host names whose every DNS address is public, the connected peer checked again, no
    /// redirects, only declared text bodies without content encoding, and at most
    /// `PUBLIC_BODY_LIMIT` bytes in 10 seconds. It keeps the run folder, raw receipts, and file
    /// sequence, and it uses no source rate-limit gates.
    pub fn public_reader(&self) -> Result<Self> {
        self.public_reader_with(PublicPolicy::default())
    }

    fn public_reader_with(&self, policy: PublicPolicy) -> Result<Self> {
        let mut clone = self.clone();
        clone.client = public_client(&policy)?;
        clone.gates = None;
        clone.hosts = HostLimits::new(self.config.concurrency);
        clone.shared = Arc::default();
        clone.config.max_body_bytes = PUBLIC_BODY_LIMIT;
        clone.public = Some(policy);
        Ok(clone)
    }

    #[cfg(test)]
    pub(crate) fn public_reader_for_test(&self, dns: TestDns) -> Result<Self> {
        let mut clone = self.public_reader_with(PublicPolicy {
            test: Some(Arc::new(dns.clone())),
        })?;
        clone.allow_loopback = dns.loopback;
        Ok(clone)
    }

    /// GET one page through a public reader, with only the fixed headers.
    pub async fn get_public(&self, url: &str) -> Result<HttpResponse> {
        anyhow::ensure!(self.public.is_some(), "Public reads need a public reader");
        #[cfg(test)]
        let url = &match self
            .public
            .as_ref()
            .and_then(|p| p.test.as_ref())
            .and_then(|t| t.port)
        {
            Some(port) => {
                let mut local = Url::parse(url).context("Invalid request URL")?;
                if local.scheme() == "https" && local.port().is_none() {
                    let _ = local.set_scheme("http");
                    let _ = local.set_port(Some(port));
                }
                local.to_string()
            }
            None => url.to_owned(),
        };
        let headers = PUBLIC_HEADERS
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        self.request_recorded(Method::GET, url, headers, None, true, SendGate::default())
            .await
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

    /// A GET that every caller in this run shares: the first call sends it, and identical later
    /// calls, including concurrent ones, receive the same response and artifact.
    pub async fn get_shared(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
    ) -> Result<HttpResponse> {
        let mut digest = Sha256::new();
        for (key, value) in &headers {
            digest.update(key.as_bytes());
            digest.update([0]);
            digest.update(value.as_bytes());
            digest.update([0]);
        }
        let key = format!("{url}\n{:x}", digest.finalize());
        let cell = self
            .shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(key)
            .or_default()
            .clone();
        cell.get_or_init(|| async {
            self.request(Method::GET, url, headers, None)
                .await
                .map_err(|e| e.to_string())
        })
        .await
        .clone()
        .map_err(anyhow::Error::msg)
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
            if self.public.is_some()
                && (!parsed.username().is_empty() || parsed.password().is_some())
            {
                return Err(Refused("the URL carries user information".into()).into());
            }
            if !parsed.username().is_empty() || parsed.password().is_some() {
                bail!("URL credentials are forbidden");
            }
            record.metadata["url"] = json!(safe_url(&parsed));
            if self.public.is_some() {
                let host = parsed.host_str().unwrap_or_default();
                if method != Method::GET || body.is_some() {
                    return Err(Refused("only GET without a body is allowed".into()).into());
                }
                if host.is_empty()
                    || host
                        .trim_start_matches('[')
                        .trim_end_matches(']')
                        .parse::<std::net::IpAddr>()
                        .is_ok()
                {
                    return Err(Refused("the URL host is not a host name".into()).into());
                }
            }
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
                    && parsed.host_str().is_some_and(|host| {
                        host == "127.0.0.1"
                            || host == "fernlet.test"
                            || host.ends_with(".fernlet.test")
                    }))
            {
                bail!("Request URL must use HTTPS");
            }
            if record.enabled {
                write_metadata(&metadata_path, &record.metadata)?;
            }
            let scope = format!("{}{}", parsed.host_str().unwrap_or_default(), parsed.path());
            let authority = format!(
                "{}:{}",
                parsed.host_str().unwrap_or_default(),
                parsed.port_or_known_default().unwrap_or_default()
            );
            let semaphore = self.hosts.semaphore(&authority);
            // Source limits are checked once the request holds its permit, at the moment of
            // sending, so a refusal seen by another request while this one queued still applies.
            // A closed scope releases the permit while it waits.
            let mut waited = Duration::ZERO;
            let mut booked = None;
            let _permit = loop {
                let permit = semaphore.clone().acquire_owned().await?;
                let Some(gates) = &self.gates else {
                    break permit;
                };
                let booked_epoch = *booked.get_or_insert_with(|| self.take_prepaid(&scope));
                let Some(wait) = gates.source_ticket(&scope, booked_epoch)? else {
                    break permit;
                };
                drop(permit);
                if waited + wait > GATE_WAIT_LIMIT {
                    self.load.rate_limited.fetch_add(1, Ordering::Relaxed);
                    return Err(SourceRateLimited { scope, wait }.into());
                }
                waited += wait;
                self.load
                    .gate_wait_ms
                    .fetch_add(wait.as_millis() as u64, Ordering::Relaxed);
                tokio::time::sleep(wait).await;
            };
            if gate.stop.is_some_and(|stop| stop()) {
                return Err(NotSent.into());
            }
            let host = parsed.host_str().unwrap_or_default().to_owned();
            if self.gates.is_some() {
                *self
                    .load
                    .sent
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .entry(host.clone())
                    .or_default() += 1;
            }
            let _in_flight = self
                .gates
                .is_some()
                .then(|| InFlight::start(&self.load, &host));
            let sent_at = Instant::now();
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
            let response = request.send().await.map_err(|e| {
                let e = e.without_url();
                if self.public.is_none() {
                    return anyhow::anyhow!("HTTP request failed: {e}");
                }
                let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&e);
                while let Some(error) = cause {
                    if let Some(answer) = error.downcast_ref::<NonPublicAnswer>() {
                        return Refused(format!("{answer}; nothing was sent")).into();
                    }
                    cause = error.source();
                }
                anyhow::anyhow!("HTTP request failed: {}", error_chain(&e))
            })?;
            let peer = response.remote_addr();
            let status = response.status().as_u16();
            record.metadata["status"] = json!(status);
            let response_headers: std::collections::BTreeMap<String, String> = response
                .headers()
                .iter()
                .filter(|(key, _)| {
                    matches!(
                        key.as_str(),
                        "content-type"
                            | "content-encoding"
                            | "content-length"
                            | "retry-after"
                            | "retry-after-ms"
                            | "date"
                            | "etag"
                            | "last-modified"
                            | "x-request-id"
                            | "x-ratelimit-limit"
                            | "x-ratelimit-remaining"
                            | "x-ratelimit-reset"
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
            if let Some(gates) = &self.gates {
                if status == 429 {
                    self.load.rate_limited.fetch_add(1, Ordering::Relaxed);
                }
                if status >= 500 {
                    self.load.server_errors.fetch_add(1, Ordering::Relaxed);
                }
                gates.observe_source(
                    &scope,
                    &source_signals(status, &response_headers),
                    GATE_DEFAULT_CLOSE,
                )?;
            }
            record.metadata["response_headers"] = json!(response_headers);
            if let Some(policy) = &self.public {
                // The resolver checked every address; the connected peer is checked again.
                if !peer.is_some_and(|p| policy.allows(p.ip())) {
                    return Err(Refused("the connected peer is not a public address".into()).into());
                }
                if (300..400).contains(&status) {
                    return Err(Refused(format!(
                        "HTTP redirect {status}; the Location was not followed"
                    ))
                    .into());
                }
                let values = |name| {
                    response
                        .headers()
                        .get_all(name)
                        .iter()
                        .map(|v| v.to_str().unwrap_or("[BINARY]").to_owned())
                        .collect::<Vec<_>>()
                };
                let encodings = values(reqwest::header::CONTENT_ENCODING);
                if encodings
                    .iter()
                    .any(|e| !e.trim().eq_ignore_ascii_case("identity"))
                {
                    return Err(Refused(format!(
                        "the body has Content-Encoding {}",
                        encodings.join(", ")
                    ))
                    .into());
                }
                let types = values(reqwest::header::CONTENT_TYPE);
                if types.len() != 1 || !PUBLIC_TYPES.contains(&media_type(&types[0]).as_str()) {
                    return Err(Refused(format!(
                        "the declared Content-Type is not one text type of {}: {}",
                        PUBLIC_TYPES.join(", "),
                        if types.is_empty() {
                            "none".to_owned()
                        } else {
                            types.join(", ")
                        }
                    ))
                    .into());
                }
            }
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
                    if self.public.is_some() {
                        return Err(Refused(format!(
                            "the body exceeds {} bytes; the retained body is incomplete",
                            self.config.max_body_bytes
                        ))
                        .into());
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
            if self.gates.is_some() {
                self.load
                    .latency_ms
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .entry(host)
                    .or_default()
                    .push(sent_at.elapsed().as_millis() as u64);
            }
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
    /// Serves `reply` to every connection after `delay`, and counts the requests.
    async fn counting_server(
        reply: &'static str,
        delay: Duration,
    ) -> (String, Arc<std::sync::atomic::AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let served = count.clone();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let served = served.clone();
                tokio::spawn(async move {
                    let mut request = vec![0; 4096];
                    let _ = socket.read(&mut request).await;
                    served.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(delay).await;
                    // Each connection serves one reply, so the client must not reuse it.
                    let reply = reply.replacen("\r\n", "\r\nConnection: close\r\n", 1);
                    let _ = socket.write_all(reply.as_bytes()).await;
                });
            }
        });
        (format!("http://{addr}"), count)
    }

    fn gated(dir: &Path) -> HttpRecorder {
        let mut recorder = HttpRecorder::new(dir, &RunConfig::default()).unwrap();
        recorder.allow_loopback = true;
        recorder.with_source_gates(Arc::new(crate::governor::Governor::local()))
    }

    #[test]
    fn a_resumed_recorder_numbers_raw_files_after_the_existing_ones() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("raw")).unwrap();
        std::fs::write(dir.path().join("raw/000007.json"), b"{}").unwrap();
        std::fs::write(dir.path().join("raw/000003.body.gz"), b"").unwrap();
        let recorder = HttpRecorder::resume(dir.path(), &RunConfig::default()).unwrap();
        assert_eq!(recorder.sequence.load(Ordering::Relaxed), 8);
        let empty = tempfile::tempdir().unwrap();
        let fresh = HttpRecorder::resume(empty.path(), &RunConfig::default()).unwrap();
        assert_eq!(fresh.sequence.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn a_refusal_closes_the_scope_and_later_requests_are_not_sent() {
        let dir = tempfile::tempdir().unwrap();
        let recorder = gated(dir.path());
        let (url, count) = counting_server(
            "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 30\r\nContent-Length: 0\r\n\r\n",
            Duration::ZERO,
        )
        .await;
        let first = recorder
            .request(Method::GET, &format!("{url}/api/search?q=a"), vec![], None)
            .await
            .unwrap();
        assert_eq!(first.status, 429);
        let error = recorder
            .request(Method::GET, &format!("{url}/api/search?q=b"), vec![], None)
            .await
            .unwrap_err();
        assert!(error.downcast_ref::<SourceRateLimited>().is_some());
        // Another path on the same host stays open.
        recorder
            .request(Method::GET, &format!("{url}/api/other"), vec![], None)
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert_eq!(recorder.load_summary()["source_rate_limited_requests"], 3);
    }

    #[tokio::test]
    async fn an_advertised_window_stops_requests_before_the_source_refuses() {
        let dir = tempfile::tempdir().unwrap();
        let recorder = gated(dir.path());
        let (url, count) = counting_server(
            "HTTP/1.1 200 OK\r\nX-RateLimit-Limit: 2\r\nX-RateLimit-Remaining: 1\r\nX-RateLimit-Reset: 60\r\nContent-Length: 2\r\n\r\n{}",
            Duration::ZERO,
        )
        .await;
        let search = format!("{url}/api/search");
        for _ in 0..2 {
            recorder
                .request(Method::GET, &search, vec![], None)
                .await
                .unwrap();
        }
        let error = recorder
            .request(Method::GET, &search, vec![], None)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Source rate limit"));
        assert_eq!(count.load(Ordering::SeqCst), 2);
        // A Jev clone never uses source gates.
        let jev = recorder.with_concurrency(4);
        jev.request(Method::GET, &search, vec![], None)
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn a_request_queued_behind_a_refusal_is_not_sent() {
        let dir = tempfile::tempdir().unwrap();
        let config = RunConfig {
            concurrency: 1,
            ..RunConfig::default()
        };
        let mut recorder = HttpRecorder::new(dir.path(), &config).unwrap();
        recorder.allow_loopback = true;
        let recorder = recorder.with_source_gates(Arc::new(crate::governor::Governor::local()));
        let (url, count) = counting_server(
            "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 30\r\nContent-Length: 0\r\n\r\n",
            Duration::from_millis(200),
        )
        .await;
        let search = format!("{url}/api/search");
        let first = recorder.request(Method::GET, &search, vec![], None);
        let queued = async {
            // Starts while the first request holds the only permit.
            tokio::time::sleep(Duration::from_millis(50)).await;
            recorder.request(Method::GET, &search, vec![], None).await
        };
        let (first, queued) = tokio::join!(first, queued);
        assert_eq!(first.unwrap().status, 429);
        assert!(queued
            .unwrap_err()
            .downcast_ref::<SourceRateLimited>()
            .is_some());
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn booked_requests_are_sent_without_counting_again() {
        let dir = tempfile::tempdir().unwrap();
        let recorder = gated(dir.path());
        let (url, count) = counting_server(
            "HTTP/1.1 200 OK\r\nX-RateLimit-Limit: 3\r\nX-RateLimit-Remaining: 2\r\nX-RateLimit-Reset: 60\r\nContent-Length: 2\r\n\r\n{}",
            Duration::ZERO,
        )
        .await;
        let search = format!("{url}/api/search");
        // Scopes name the host and path, without the port.
        let scope = "127.0.0.1/api/search".to_owned();
        // The first response teaches the window: 1 of 3 used.
        recorder
            .request(Method::GET, &search, vec![], None)
            .await
            .unwrap();
        assert_eq!(
            recorder.book_windows(&[(scope.clone(), 2)]).await.unwrap(),
            None
        );
        assert!(recorder.take_prepaid(&scope).is_some());
        // Give the checked ticket back: the two requests below spend the booking.
        recorder.prepaid.lock().unwrap().get_mut(&scope).unwrap().0 += 1;
        for _ in 0..2 {
            recorder
                .request(Method::GET, &search, vec![], None)
                .await
                .unwrap();
        }
        assert_eq!(count.load(Ordering::SeqCst), 3);
        // The window is full: an unbooked request is refused, and a booking would wait.
        assert!(recorder
            .request(Method::GET, &search, vec![], None)
            .await
            .is_err());
        assert_eq!(recorder.load_summary()["source_booking_wait_ms"], 0);
        // A question that cannot book in time is told how long to wait, and books nothing.
        let wait = recorder
            .book_windows_within(&[(scope.clone(), 1)], Duration::from_millis(100))
            .await
            .unwrap()
            .expect("no room in time");
        assert!(wait > Duration::from_secs(50));
    }

    #[tokio::test]
    async fn source_latency_counts_completed_and_peak_in_flight_per_host() {
        let dir = tempfile::tempdir().unwrap();
        let recorder = gated(dir.path());
        let (url, _count) = counting_server(
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}",
            Duration::from_millis(40),
        )
        .await;
        let search = format!("{url}/api/find");
        let requests = (0..3).map(|_| recorder.request(Method::GET, &search, vec![], None));
        for result in futures::future::join_all(requests).await {
            result.unwrap();
        }
        let latency = &recorder.load_summary()["source_latency"]["127.0.0.1"];
        assert_eq!(latency["completed"], 3);
        assert_eq!(latency["not_completed"], 0);
        assert!(latency["p50_ms"].as_u64().unwrap() >= 30);
        assert!(latency["peak_in_flight"].as_u64().unwrap() >= 2);
    }

    #[test]
    fn rate_limit_headers_never_overflow_and_long_waits_are_kept() {
        let headers = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<std::collections::BTreeMap<_, _>>()
        };
        let huge = source_signals(429, &headers(&[("retry-after", "1e30")]));
        assert_eq!(huge.retry_after, Some(GATE_MAX_CLOSE));
        let long = source_signals(429, &headers(&[("retry-after", "300")]));
        assert_eq!(long.retry_after, Some(Duration::from_secs(300)));
        let longest = source_signals(429, &headers(&[("retry-after", "3600")]));
        assert_eq!(longest.retry_after, Some(GATE_MAX_CLOSE));
        // HTTP-dates: a future one waits until then, a past one not at all.
        let future = source_signals(
            429,
            &headers(&[("retry-after", "Sun, 06 Nov 2044 08:49:37 GMT")]),
        );
        assert_eq!(future.retry_after, Some(GATE_MAX_CLOSE));
        let past = source_signals(
            429,
            &headers(&[("retry-after", "Wed, 21 Oct 2015 07:28:00 GMT")]),
        );
        assert_eq!(past.retry_after, Some(Duration::ZERO));
        assert_eq!(
            http_date_seconds("Mon, 01 Jan 2024 00:00:10 GMT"),
            Some(1_704_067_210)
        );
        for bad in ["-5", "NaN", "inf", "Wed, 32 Oct 2026 07:28:00 GMT", "soon"] {
            let signals = source_signals(429, &headers(&[("retry-after", bad)]));
            assert_eq!(signals.retry_after, None, "{bad}");
        }
        let windowed = source_signals(
            200,
            &headers(&[
                ("x-ratelimit-limit", "1e30"),
                ("x-ratelimit-remaining", "5"),
                ("x-ratelimit-reset", "9e99"),
            ]),
        );
        assert_eq!(windowed.limit, None);
        assert_eq!(windowed.remaining, Some(5));
        assert!(windowed.reset_ms.is_some());
    }

    #[tokio::test]
    async fn identical_gets_in_one_run_share_one_response() {
        let dir = tempfile::tempdir().unwrap();
        let mut recorder = HttpRecorder::new(dir.path(), &RunConfig::default()).unwrap();
        recorder.allow_loopback = true;
        let (url, count) = counting_server(
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}",
            Duration::from_millis(100),
        )
        .await;
        let headers = || vec![("x-key".to_owned(), "a".to_owned())];
        let (a, b) = tokio::join!(
            recorder.get_shared(&url, headers()),
            recorder.get_shared(&url, headers())
        );
        assert_eq!(a.unwrap().artifact, b.unwrap().artifact);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        // Different headers are a different request.
        recorder
            .get_shared(&url, vec![("x-key".into(), "b".into())])
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_slow_host_does_not_hold_the_permits_of_another_host() {
        let dir = tempfile::tempdir().unwrap();
        let config = RunConfig {
            concurrency: 1,
            ..RunConfig::default()
        };
        let mut recorder = HttpRecorder::new(dir.path(), &config).unwrap();
        recorder.allow_loopback = true;
        let (slow, _) = counting_server(
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
            Duration::from_secs(2),
        )
        .await;
        let (fast, _) = counting_server(
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
            Duration::ZERO,
        )
        .await;
        let slow_request = recorder.request(Method::GET, &slow, vec![], None);
        let fast_request = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let started = Instant::now();
            recorder
                .request(Method::GET, &fast, vec![], None)
                .await
                .unwrap();
            started.elapsed()
        };
        let (_, fast_elapsed) = tokio::join!(slow_request, fast_request);
        assert!(fast_elapsed < Duration::from_secs(1));
    }

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
    /// Serves `reply` to every connection, counts requests, and sends each request's head.
    async fn page_server(
        reply: Vec<u8>,
        delay_after: Option<usize>,
    ) -> (
        u16,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::sync::mpsc::UnboundedReceiver<String>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let served = count.clone();
        let reply = Arc::new(reply);
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (served, sender, reply) = (served.clone(), sender.clone(), reply.clone());
                tokio::spawn(async move {
                    let mut request = vec![0; 8192];
                    let n = socket.read(&mut request).await.unwrap_or(0);
                    served.fetch_add(1, Ordering::SeqCst);
                    let _ = sender.send(String::from_utf8_lossy(&request[..n]).into_owned());
                    let split = delay_after.unwrap_or(reply.len()).min(reply.len());
                    let _ = socket.write_all(&reply[..split]).await;
                    if split < reply.len() {
                        let _ = socket.flush().await;
                        tokio::time::sleep(Duration::from_secs(10)).await;
                        let _ = socket.write_all(&reply[split..]).await;
                    }
                });
            }
        });
        (port, count, receiver)
    }

    fn reply(head: &str, body: &[u8]) -> Vec<u8> {
        let mut bytes = format!(
            "HTTP/1.1 {head}\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        bytes.extend_from_slice(body);
        bytes
    }

    fn reader(dir: &Path, port: u16, answers: &[(&str, &[&str])]) -> HttpRecorder {
        let mut dns = TestDns {
            loopback: true,
            port: Some(port),
            ..TestDns::default()
        };
        dns.answers
            .insert("fernlet.test".into(), vec!["127.0.0.1".parse().unwrap()]);
        for (host, ips) in answers {
            dns.answers.insert(
                (*host).into(),
                ips.iter().map(|ip| ip.parse().unwrap()).collect(),
            );
        }
        HttpRecorder::new(dir, &RunConfig::default())
            .unwrap()
            .public_reader_for_test(dns)
            .unwrap()
    }

    fn refusal(result: Result<HttpResponse>) -> String {
        let error = result.unwrap_err();
        error
            .downcast_ref::<Refused>()
            .unwrap_or_else(|| panic!("not a refusal: {error}"))
            .to_string()
    }

    #[test]
    fn only_public_unicast_addresses_pass_the_filter() {
        for public in ["93.184.216.34", "2606:4700::1111", "::ffff:93.184.216.34"] {
            assert!(public_address(public.parse().unwrap()), "{public}");
        }
        for refused in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "100.100.100.200",
            "0.0.0.0",
            "224.0.0.1",
            "255.255.255.255",
            "240.0.0.1",
            "192.0.2.1",
            "198.51.100.1",
            "203.0.113.1",
            "198.18.0.1",
            "192.0.0.192",
            "::1",
            "::",
            "ff02::1",
            "fc00::1",
            "fd00:ec2::254",
            "fe80::1",
            "2001:db8::1",
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.1",
            "64:ff9b::a9fe:a9fe",
        ] {
            assert!(!public_address(refused.parse().unwrap()), "{refused}");
        }
    }

    #[tokio::test]
    async fn answer_sets_with_any_non_public_address_are_refused_before_connect() {
        let dir = tempfile::tempdir().unwrap();
        let (port, count, _) =
            page_server(reply("200 OK\r\nContent-Type: text/plain", b"ok"), None).await;
        let reader = reader(
            dir.path(),
            port,
            &[
                ("private.fernlet.test", &["10.1.2.3"]),
                ("mixed.fernlet.test", &["127.0.0.1", "192.168.4.4"]),
                ("mapped.fernlet.test", &["::ffff:10.9.9.9"]),
                ("metadata.fernlet.test", &["169.254.169.254"]),
                ("carrier.fernlet.test", &["100.100.100.200"]),
                ("ula.fernlet.test", &["fd00:ec2::254"]),
                ("empty.fernlet.test", &[]),
            ],
        );
        for host in [
            "private", "mixed", "mapped", "metadata", "carrier", "ula", "empty",
        ] {
            let message = refusal(
                reader
                    .get_public(&format!("https://{host}.fernlet.test/page"))
                    .await,
            );
            assert!(message.contains("DNS answer"), "{host}: {message}");
        }
        assert_eq!(count.load(Ordering::SeqCst), 0);
        let page = reader
            .get_public("https://fernlet.test/page")
            .await
            .unwrap();
        assert_eq!(page.body, b"ok");
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn literal_address_hosts_and_user_information_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (port, count, _) =
            page_server(reply("200 OK\r\nContent-Type: text/plain", b"ok"), None).await;
        let reader = reader(dir.path(), port, &[]);
        for url in [
            "https://127.0.0.1/page",
            "https://[::1]/page",
            "https://reader:secret@fernlet.test/page",
        ] {
            refusal(reader.get_public(url).await);
        }
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn redirects_are_refused_without_following_location() {
        for status in [
            "301 Moved Permanently",
            "302 Found",
            "307 Temporary Redirect",
            "308 Permanent Redirect",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let (port, count, _) = page_server(
                reply(
                    &format!("{status}\r\nLocation: /elsewhere\r\nContent-Type: text/html"),
                    b"moved",
                ),
                None,
            )
            .await;
            let reader = reader(dir.path(), port, &[]);
            let message = refusal(reader.get_public("https://fernlet.test/page").await);
            assert!(message.contains("redirect"), "{message}");
            assert_eq!(count.load(Ordering::SeqCst), 1, "{status}");
            let metadata: Value =
                serde_json::from_slice(&std::fs::read(dir.path().join("raw/000000.json")).unwrap())
                    .unwrap();
            assert!(metadata["failure"].as_str().unwrap().contains("redirect"));
        }
    }

    #[tokio::test]
    async fn a_public_read_sends_only_the_fixed_headers() {
        let dir = tempfile::tempdir().unwrap();
        let (port, _, mut requests) = page_server(
            reply("200 OK\r\nContent-Type: text/html", b"<main>ok</main>"),
            None,
        )
        .await;
        let reader = reader(dir.path(), port, &[]);
        reader
            .get_public("https://fernlet.test/page")
            .await
            .unwrap();
        let head = requests.recv().await.unwrap();
        let names: std::collections::BTreeSet<String> = head
            .lines()
            .skip(1)
            .filter_map(|line| line.split_once(':'))
            .map(|(name, _)| name.trim().to_ascii_lowercase())
            .collect();
        assert_eq!(
            names,
            std::collections::BTreeSet::from(
                ["host", "accept", "accept-encoding", "user-agent"].map(String::from)
            )
        );
        assert!(head
            .to_ascii_lowercase()
            .contains("accept-encoding: identity"));
    }

    #[tokio::test]
    async fn encoded_untyped_and_unsupported_bodies_are_refused() {
        for head in [
            "200 OK\r\nContent-Type: text/html\r\nContent-Encoding: gzip",
            "200 OK\r\nContent-Type: application/json",
            "200 OK",
            "200 OK\r\nContent-Type: text/html\r\nContent-Type: text/plain",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let (port, _, _) = page_server(reply(head, b"body"), None).await;
            let reader = reader(dir.path(), port, &[]);
            refusal(reader.get_public("https://fernlet.test/page").await);
        }
        let dir = tempfile::tempdir().unwrap();
        let (port, _, _) = page_server(
            reply(
                "200 OK\r\nContent-Type: text/markdown; charset=utf-8",
                b"# Page",
            ),
            None,
        )
        .await;
        let reader = reader(dir.path(), port, &[]);
        assert!(reader.get_public("https://fernlet.test/page").await.is_ok());
    }

    #[tokio::test]
    async fn a_body_past_the_limit_is_cut_and_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let body = vec![b'a'; PUBLIC_BODY_LIMIT + 10];
        let (port, _, _) =
            page_server(reply("200 OK\r\nContent-Type: text/plain", &body), None).await;
        let reader = reader(dir.path(), port, &[]);
        let message = refusal(reader.get_public("https://fernlet.test/page").await);
        assert!(message.contains("exceeds"), "{message}");
        let metadata: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("raw/000000.json")).unwrap())
                .unwrap();
        assert_eq!(metadata["retained_bytes"], PUBLIC_BODY_LIMIT);
        assert_eq!(metadata["complete"], false);
        assert_eq!(
            raw_body(&dir.path().join("raw/000000.body.gz")).len(),
            PUBLIC_BODY_LIMIT
        );
    }

    #[tokio::test]
    async fn a_slow_body_times_out_with_a_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let full = reply("200 OK\r\nContent-Type: text/plain", b"abcdef");
        let (port, _, _) = page_server(full.clone(), Some(full.len() - 3)).await;
        let mut dns = TestDns {
            loopback: true,
            port: Some(port),
            timeout: Some(Duration::from_secs(1)),
            ..TestDns::default()
        };
        dns.answers
            .insert("fernlet.test".into(), vec!["127.0.0.1".parse().unwrap()]);
        let reader = HttpRecorder::new(dir.path(), &RunConfig::default())
            .unwrap()
            .public_reader_for_test(dns)
            .unwrap();
        let started = Instant::now();
        let error = reader
            .get_public("https://fernlet.test/page")
            .await
            .unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(
            error.to_string().contains("Response body failed"),
            "{error}"
        );
        let metadata: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("raw/000000.json")).unwrap())
                .unwrap();
        assert_eq!(metadata["complete"], false);
        assert_eq!(metadata["retained_bytes"], 3);
        assert!(metadata["failure"].is_string());
    }

    #[tokio::test]
    async fn the_production_public_reader_refuses_loopback() {
        let dir = tempfile::tempdir().unwrap();
        let reader = HttpRecorder::new(dir.path(), &RunConfig::default())
            .unwrap()
            .public_reader()
            .unwrap();
        let message = refusal(reader.get_public("https://localhost/page").await);
        assert!(message.contains("DNS answer"), "{message}");
        refusal(reader.get_public("https://127.0.0.1/page").await);
        // Plain HTTP never leaves the recorder.
        assert!(reader.get_public("http://localhost/page").await.is_err());
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
