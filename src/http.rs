use crate::types::RunConfig;
use anyhow::{bail, Context, Result};
use futures::StreamExt;
use reqwest::{redirect::Policy, Client, Method, Url};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
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
    /// Hedges for stalled source GETs: a client that opens a fresh connection for each request,
    /// and hedge permits per host apart from the host's request permits.
    hedge_client: Client,
    hedge_hosts: Arc<HostLimits>,
    /// Set on the clone that sends a hedge, so its receipt says so.
    hedge: bool,
    /// The network check's client: the request client's settings with a connect limit.
    probe_client: Client,
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

/// The network check's connect limit. It covers name resolution, a proxy tunnel, and TLS.
const PROBE_CONNECT_LIMIT: Duration = Duration::from_secs(5);
/// The network check's wait for an answer. It exceeds the connect limit, so a probe that still
/// waits at this point has a connection: the path works, and only the answer is slow.
const PROBE_WAIT: Duration = Duration::from_secs(6);

/// One network check: the HTTP status the origin answered with, `None` when it connected but has
/// not answered within `PROBE_WAIT`, or the cause class of the failure.
pub type ProbeResult = Result<Option<u16>, TransportCause>;

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
            // Local-use NAT64 (64:ff9b:1::/48), 6to4 (2002::/16), and Teredo (2001::/32) can
            // reach an IPv4 destination this check cannot see.
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (s[0] == 0x64 && s[1] == 0xff9b && s[2] == 1)
                || s[0] == 0x2002
                || (s[0] == 0x2001 && s[1] == 0)
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

/// The builder for source and Jev requests: no redirects and the configured time limit. A `fresh`
/// client keeps no idle connection, so each request opens its own.
fn source_client(config: &RunConfig, fresh: bool) -> reqwest::ClientBuilder {
    let builder = Client::builder()
        .redirect(Policy::none())
        .timeout(Duration::from_secs(config.timeout_secs));
    if fresh {
        builder.pool_max_idle_per_host(0)
    } else {
        builder
    }
}

/// The network check's client from a request client's builder: a fresh connection for each check,
/// the connect limit, and a total limit past `PROBE_WAIT`. The request limit (`--timeout-secs`)
/// does not apply, so a short one cannot turn a slow answer into a failed check.
fn probe_client(builder: reqwest::ClientBuilder) -> Result<Client> {
    Ok(builder
        .connect_timeout(PROBE_CONNECT_LIMIT)
        .timeout(PROBE_WAIT * 2)
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
/// Hedges one run may send, and hedges in flight per host. Hedges never take the permits of the
/// requests they duplicate.
const HEDGES_PER_RUN: u64 = 16;
const HEDGES_PER_HOST: usize = 2;
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
    /// Hedge requests sent in this run.
    hedges: AtomicU64,
    /// Hedges sent, and hedges whose response was returned, per host.
    hedged: Mutex<std::collections::BTreeMap<String, (u64, u64)>>,
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
/// `sending` is set just before the built request goes to the HTTP client. An error while it is
/// still unset is a local refusal or failure, and nothing was sent.
#[derive(Clone, Copy, Default)]
pub struct SendGate<'a> {
    pub stop: Option<&'a (dyn Fn() -> bool + Sync)>,
    pub notify: Option<&'a tokio::sync::Notify>,
    pub sending: Option<&'a AtomicBool>,
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

/// Why a request failed before a complete response, as a fixed class. It names no URL, header,
/// credential, or body, so failure records and output can carry it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportCause {
    /// The host name did not resolve.
    Dns,
    /// The host refused the connection.
    ConnectRefused,
    /// The system or a proxy refused or could not open the connection. A proxy that cannot reach
    /// the host gives this class too.
    ConnectDenied,
    /// No route to the network or the host.
    Unreachable,
    /// Another failure while connecting.
    Connect,
    /// The TLS handshake or the certificate check failed.
    Tls,
    /// The connection or the response did not finish in time.
    Timeout,
    /// The connection closed or reset before the response ended.
    Closed,
    /// Any other failure before a complete response.
    Other,
}

impl TransportCause {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Dns => "dns",
            Self::ConnectRefused => "connect_refused",
            Self::ConnectDenied => "connect_denied",
            Self::Unreachable => "unreachable",
            Self::Connect => "connect",
            Self::Tls => "tls",
            Self::Timeout => "timeout",
            Self::Closed => "connection_closed",
            Self::Other => "other",
        }
    }
}

impl std::fmt::Display for TransportCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Fixed messages of connector and protocol errors that have no public type, with their class.
const LIBRARY_MESSAGES: &[(&str, TransportCause)] = &[
    ("dns error", TransportCause::Dns),
    ("tunnel error: unsuccessful", TransportCause::ConnectDenied),
    (
        "tunnel error: proxy authorization required",
        TransportCause::ConnectDenied,
    ),
    (
        "connection closed before message completed",
        TransportCause::Closed,
    ),
];

/// The class of a failed request, from the error's typed parts: the timeout flag, I/O error kinds,
/// and the fixed messages in `LIBRARY_MESSAGES`. The error text itself is never kept.
fn transport_cause(error: &reqwest::Error) -> TransportCause {
    use std::io::ErrorKind as Kind;
    if error.is_timeout() {
        return TransportCause::Timeout;
    }
    let mut kind = None;
    let mut layer: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(current) = layer {
        let text = current.to_string();
        if let Some((_, cause)) = LIBRARY_MESSAGES.iter().find(|(m, _)| text == *m) {
            return *cause;
        }
        if let Some(io) = current.downcast_ref::<std::io::Error>() {
            kind = Some(io.kind());
            // `source` of a wrapping I/O error skips the error it wraps.
            if let Some(inner) = io.get_ref() {
                layer = Some(inner);
                continue;
            }
        }
        layer = current.source();
    }
    let connect = error.is_connect();
    match kind {
        Some(Kind::ConnectionRefused) => TransportCause::ConnectRefused,
        Some(Kind::PermissionDenied) => TransportCause::ConnectDenied,
        Some(
            Kind::NetworkUnreachable
            | Kind::HostUnreachable
            | Kind::NetworkDown
            | Kind::AddrNotAvailable,
        ) => TransportCause::Unreachable,
        Some(Kind::NotConnected) if connect => TransportCause::Unreachable,
        Some(Kind::TimedOut) => TransportCause::Timeout,
        Some(Kind::InvalidData) if connect => TransportCause::Tls,
        Some(
            Kind::ConnectionReset
            | Kind::ConnectionAborted
            | Kind::BrokenPipe
            | Kind::UnexpectedEof
            | Kind::NotConnected,
        ) => TransportCause::Closed,
        _ if connect => TransportCause::Connect,
        _ => TransportCause::Other,
    }
}

/// A request that failed before a complete response: its class, whether it was sent, and the
/// transport message without the URL.
#[derive(Debug)]
pub struct TransportFailure {
    pub cause: TransportCause,
    /// The connection failed before the request was written, so the host received no request
    /// bytes. `reqwest` marks only failures of the connect phase this way: name resolution, the
    /// TCP connection, a proxy tunnel, and the TLS handshake. The class does not decide this; a
    /// closed connection can also come before the request. A connection still not open at the
    /// request time limit is not marked, because that limit is not a connect-phase error.
    pub unsent: bool,
    text: String,
}

impl TransportFailure {
    fn new(error: &reqwest::Error, text: String) -> Self {
        Self {
            cause: transport_cause(error),
            unsent: error.is_connect(),
            text,
        }
    }
}

impl std::fmt::Display for TransportFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}

impl std::error::Error for TransportFailure {}

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
        recorder.client = source_client(config, false).no_proxy().build()?;
        recorder.hedge_client = source_client(config, true).no_proxy().build()?;
        recorder.probe_client = probe_client(source_client(config, true).no_proxy())?;
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
    /// sent that did not complete (failed, cut, or a cancelled hedge race loser), the most requests
    /// in flight at once, hedges sent, and hedges whose response was returned.
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
        let hedged = self.load.hedged.lock().unwrap_or_else(|e| e.into_inner());
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
                        "hedged": hedged.get(host).map_or(0, |entry| entry.0),
                        "hedge_wins": hedged.get(host).map_or(0, |entry| entry.1),
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
            client: source_client(config, false).build()?,
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
            hedge_client: source_client(config, true).build()?,
            hedge_hosts: HostLimits::new(HEDGES_PER_HOST),
            hedge: false,
            probe_client: probe_client(source_client(config, true))?,
        })
    }

    /// Offline tests only: every request goes through the proxy at `proxy`.
    #[cfg(test)]
    pub(crate) fn through_proxy_for_test(&self, proxy: &str) -> Result<Self> {
        let mut clone = self.clone();
        let proxy = reqwest::Proxy::all(proxy)?;
        clone.client = source_client(&self.config, false)
            .proxy(proxy.clone())
            .build()?;
        clone.probe_client = probe_client(source_client(&self.config, true).proxy(proxy))?;
        Ok(clone)
    }

    /// Offline tests only: no host name resolves.
    #[cfg(test)]
    pub(crate) fn without_dns_for_test(&self) -> Result<Self> {
        struct NoAnswer;
        impl reqwest::dns::Resolve for NoAnswer {
            fn resolve(&self, _: reqwest::dns::Name) -> reqwest::dns::Resolving {
                Box::pin(async { Err("no answer".into()) })
            }
        }
        self.with_resolver_for_test(Arc::new(NoAnswer))
    }

    /// Offline tests only: requests and network checks resolve names through `resolver`.
    #[cfg(test)]
    pub(crate) fn with_resolver_for_test<R: reqwest::dns::Resolve + 'static>(
        &self,
        resolver: Arc<R>,
    ) -> Result<Self> {
        let mut clone = self.clone();
        clone.client = source_client(&self.config, false)
            .no_proxy()
            .dns_resolver(resolver.clone())
            .build()?;
        clone.probe_client = probe_client(
            source_client(&self.config, true)
                .no_proxy()
                .dns_resolver(resolver),
        )?;
        Ok(clone)
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

    /// A source GET without a body, on the clone with source gates, is hedged: see
    /// `request_hedged`.
    pub async fn request(
        &self,
        method: Method,
        url: &str,
        headers: Vec<(String, String)>,
        body: Option<Value>,
    ) -> Result<HttpResponse> {
        if self.gates.is_some()
            && method == Method::GET
            && body.is_none()
            && !self.config.fixture
            && self.config.source_hedge_ms > 0
        {
            let after = Duration::from_millis(self.config.source_hedge_ms);
            return self.request_hedged(url, headers, after).await;
        }
        self.request_recorded(method, url, headers, body, true, SendGate::default())
            .await
    }

    /// A GET that gets one hedge, the same request on a fresh connection, when it has no response
    /// `after` it was sent. The hedge waits for a hedge permit on its host, passes the source gates
    /// like any request, and is sent only while the run has hedges left. The first decisive
    /// response wins: one that is not an error, a 429, or a 5xx. The other request is cancelled,
    /// and its receipt is kept. When neither is decisive, the original's result stands, so the
    /// caller's own retry still applies.
    async fn request_hedged(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        after: Duration,
    ) -> Result<HttpResponse> {
        let sent = tokio::sync::Notify::new();
        let first = self.request_recorded(
            Method::GET,
            url,
            headers.clone(),
            None,
            true,
            SendGate {
                notify: Some(&sent),
                ..SendGate::default()
            },
        );
        tokio::pin!(first);
        // Queue and gate waits do not count: the delay starts when the original is sent.
        let delay = async {
            sent.notified().await;
            tokio::time::sleep(after).await;
        };
        tokio::select! {
            biased;
            result = &mut first => return result,
            _ = delay => {}
        }
        if self.load.hedges.load(Ordering::SeqCst) >= HEDGES_PER_RUN {
            return first.await;
        }
        let host = Url::parse(url)
            .ok()
            .and_then(|u| u.host_str().map(str::to_owned))
            .unwrap_or_default();
        let load = &self.load;
        let hedge_sent = AtomicBool::new(false);
        // Checked when the hedge is about to be sent, so a hedge that never leaves costs nothing.
        let take = || {
            let taken = load
                .hedges
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                    (n < HEDGES_PER_RUN).then_some(n + 1)
                })
                .is_ok();
            if taken {
                hedge_sent.store(true, Ordering::SeqCst);
                load.hedged
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .entry(host.clone())
                    .or_default()
                    .0 += 1;
            }
            !taken
        };
        let hedger = Self {
            client: self.hedge_client.clone(),
            hosts: self.hedge_hosts.clone(),
            hedge: true,
            // A hedge always counts at send; it never spends another request's booked ticket.
            prepaid: Arc::default(),
            ..self.clone()
        };
        let second = hedger.request_recorded(
            Method::GET,
            url,
            headers,
            None,
            true,
            SendGate {
                stop: Some(&take),
                ..SendGate::default()
            },
        );
        tokio::pin!(second);
        let decisive =
            |r: &Result<HttpResponse>| matches!(r, Ok(r) if r.status != 429 && r.status < 500);
        let (result, hedge_won) = tokio::select! {
            result = &mut first => {
                if decisive(&result) || !hedge_sent.load(Ordering::SeqCst) {
                    (result, false)
                } else {
                    let other = (&mut second).await;
                    if decisive(&other) {
                        (other, true)
                    } else {
                        (result, false)
                    }
                }
            }
            result = &mut second => {
                if decisive(&result) {
                    (result, true)
                } else {
                    ((&mut first).await, false)
                }
            }
        };
        if hedge_won {
            load.hedged
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .entry(host.clone())
                .or_default()
                .1 += 1;
        }
        result
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

    /// One HEAD request to `origin` without headers or body, for a free reachability check: it
    /// carries no credentials, so no provider can bill it. Any HTTP status means the network path
    /// works, and so does a connection still waiting for its answer at `PROBE_WAIT`. It keeps no
    /// raw receipt and passes no host permit or rate-limit gate.
    pub async fn probe(&self, origin: &str) -> ProbeResult {
        let url = Url::parse(origin).map_err(|_| TransportCause::Other)?;
        let allowed = url.scheme() == "https" || (self.allow_loopback && url.scheme() == "http");
        if self.config.fixture || !allowed {
            return Err(TransportCause::Other);
        }
        match tokio::time::timeout(PROBE_WAIT, self.probe_client.head(url).send()).await {
            Err(_) => Ok(None),
            Ok(Err(error)) => Err(transport_cause(&error)),
            Ok(Ok(response)) => Ok(Some(response.status().as_u16())),
        }
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
        if self.hedge {
            record.metadata["hedge"] = json!(true);
        }
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
            // Source requests and public reads count per host; Jev requests keep their own usage.
            let accounted = self.gates.is_some() || self.public.is_some();
            if accounted {
                *self
                    .load
                    .sent
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .entry(host.clone())
                    .or_default() += 1;
            }
            let _in_flight = accounted.then(|| InFlight::start(&self.load, &host));
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
            let request = request.build().map_err(|e| {
                anyhow::anyhow!(
                    "HTTP request could not be built: {}",
                    error_chain(&e.without_url())
                )
            })?;
            if let Some(sending) = gate.sending {
                sending.store(true, Ordering::SeqCst);
            }
            let response = self
                .client
                .execute(request)
                .await
                .map_err(|e| -> anyhow::Error {
                    let e = e.without_url();
                    if self.public.is_none() {
                        return TransportFailure::new(&e, format!("HTTP request failed: {e}"))
                            .into();
                    }
                    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&e);
                    while let Some(error) = cause {
                        if let Some(answer) = error.downcast_ref::<NonPublicAnswer>() {
                            return Refused(format!("{answer}; nothing was sent")).into();
                        }
                        cause = error.source();
                    }
                    TransportFailure::new(&e, format!("HTTP request failed: {}", error_chain(&e)))
                        .into()
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
                let chunk = chunk.map_err(|e| {
                    let e = e.without_url();
                    TransportFailure::new(&e, format!("Response body failed: {e}"))
                })?;
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
            if accounted {
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

    const OK: &str = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}";
    const NEVER: Option<Duration> = None;

    fn after(ms: u64) -> Option<Duration> {
        Some(Duration::from_millis(ms))
    }

    /// Answers each request, in arrival order, after its script delay with its script reply;
    /// `NEVER` holds the connection without an answer. Requests past the script get a 200 at
    /// once. Returns the URL, the request count, and each request's head.
    async fn scripted_server(
        script: Vec<(Option<Duration>, &'static str)>,
    ) -> (
        String,
        Arc<std::sync::atomic::AtomicUsize>,
        Arc<Mutex<Vec<String>>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let heads = Arc::new(Mutex::new(Vec::new()));
        let (served, seen, script) = (count.clone(), heads.clone(), Arc::new(script));
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (served, seen, script) = (served.clone(), seen.clone(), script.clone());
                tokio::spawn(async move {
                    let mut request = vec![0; 4096];
                    let n = socket.read(&mut request).await.unwrap_or(0);
                    seen.lock()
                        .unwrap()
                        .push(String::from_utf8_lossy(&request[..n]).into_owned());
                    let index = served.fetch_add(1, Ordering::SeqCst);
                    let (delay, reply) = script
                        .get(index)
                        .copied()
                        .unwrap_or((Some(Duration::ZERO), OK));
                    tokio::time::sleep(delay.unwrap_or(Duration::from_secs(60))).await;
                    let reply = reply.replacen("\r\n", "\r\nConnection: close\r\n", 1);
                    let _ = socket.write_all(reply.as_bytes()).await;
                });
            }
        });
        (format!("http://{addr}/api/find"), count, heads)
    }

    fn hedging(dir: &Path, config: RunConfig) -> HttpRecorder {
        let mut recorder = HttpRecorder::new(dir, &config).unwrap();
        recorder.allow_loopback = true;
        recorder.with_source_gates(Arc::new(crate::governor::Governor::local()))
    }

    fn hedge_after(ms: u64) -> RunConfig {
        RunConfig {
            source_hedge_ms: ms,
            ..RunConfig::default()
        }
    }

    /// Raw receipts in sequence order.
    fn receipts(dir: &Path) -> Vec<Value> {
        let mut names: Vec<_> = std::fs::read_dir(dir.join("raw"))
            .unwrap()
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .filter(|name| name.ends_with(".json"))
            .collect();
        names.sort();
        names
            .iter()
            .map(|name| serde_json::from_slice(&std::fs::read(dir.join("raw").join(name)).unwrap()))
            .collect::<Result<_, _>>()
            .unwrap()
    }

    #[tokio::test]
    async fn a_stalled_source_get_is_hedged_with_the_same_request_and_the_hedge_wins() {
        let dir = tempfile::tempdir().unwrap();
        let recorder = hedging(dir.path(), hedge_after(200));
        let (url, count, heads) = scripted_server(vec![(NEVER, OK), (after(100), OK)]).await;
        let started = Instant::now();
        let response = recorder
            .request(
                Method::GET,
                &format!("{url}?q=quillon"),
                vec![("x-fernlet".into(), "7".into())],
                None,
            )
            .await
            .unwrap();
        let elapsed = started.elapsed();
        assert_eq!(response.status, 200);
        assert!(
            elapsed >= Duration::from_millis(300) && elapsed < Duration::from_secs(2),
            "{elapsed:?}"
        );
        assert_eq!(count.load(Ordering::SeqCst), 2);
        let heads = heads.lock().unwrap().clone();
        for head in &heads {
            assert!(
                head.starts_with("GET /api/find?q=quillon HTTP/1.1"),
                "{head}"
            );
            assert!(head.to_ascii_lowercase().contains("x-fernlet: 7"), "{head}");
        }
        let load = recorder.load_summary();
        assert_eq!(load["source_requests"]["127.0.0.1"], 2);
        let latency = &load["source_latency"]["127.0.0.1"];
        assert_eq!(latency["hedged"], 1);
        assert_eq!(latency["hedge_wins"], 1);
        assert_eq!(latency["completed"], 1);
        assert_eq!(latency["not_completed"], 1);
        // Both attempts keep a receipt; the cancelled original says so.
        let receipts = receipts(dir.path());
        assert_eq!(receipts.len(), 2);
        assert!(receipts[0].get("hedge").is_none());
        assert_eq!(receipts[0]["complete"], false);
        assert!(receipts[0]["failure"]
            .as_str()
            .unwrap()
            .contains("cancelled"));
        assert_eq!(receipts[1]["hedge"], true);
        assert_eq!(receipts[1]["complete"], true);
        assert_eq!(response.artifact, "raw/000001.body.gz");
    }

    #[tokio::test]
    async fn a_hedge_that_answers_first_wins_and_the_slower_original_is_cancelled() {
        let dir = tempfile::tempdir().unwrap();
        let recorder = hedging(dir.path(), hedge_after(200));
        let (url, _, _) = scripted_server(vec![(after(1500), OK), (after(300), OK)]).await;
        let started = Instant::now();
        let response = recorder
            .request(Method::GET, &url, vec![], None)
            .await
            .unwrap();
        assert!(started.elapsed() < Duration::from_millis(1200));
        assert_eq!(response.artifact, "raw/000001.body.gz");
        let latency = &recorder.load_summary()["source_latency"]["127.0.0.1"];
        assert_eq!(latency["hedged"], 1);
        assert_eq!(latency["hedge_wins"], 1);
        let original = &receipts(dir.path())[0];
        assert!(original["failure"].as_str().unwrap().contains("cancelled"));
    }

    #[tokio::test]
    async fn source_hedging_is_off_by_default() {
        assert_eq!(RunConfig::default().source_hedge_ms, 0);
        let dir = tempfile::tempdir().unwrap();
        let recorder = hedging(dir.path(), RunConfig::default());
        let (url, count, _) = scripted_server(vec![(after(1000), OK)]).await;
        recorder
            .request(Method::GET, &url, vec![], None)
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(
            recorder.load_summary()["source_latency"]["127.0.0.1"]["hedged"],
            0
        );
    }

    #[tokio::test]
    async fn a_fast_get_a_post_and_fixture_mode_get_no_hedge() {
        let dir = tempfile::tempdir().unwrap();
        let recorder = hedging(dir.path(), hedge_after(300));
        let (url, count, _) = scripted_server(vec![(after(50), OK)]).await;
        recorder
            .request(Method::GET, &url, vec![], None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(count.load(Ordering::SeqCst), 1, "answered before the delay");

        let recorder = hedging(dir.path(), hedge_after(100));
        let (url, count, _) = scripted_server(vec![(after(500), OK)]).await;
        recorder
            .request(Method::POST, &url, vec![], Some(json!({"q": "quillon"})))
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1, "a POST is never hedged");

        let fixture = tempfile::tempdir().unwrap();
        let recorder = hedging(
            fixture.path(),
            RunConfig {
                fixture: true,
                ..hedge_after(100)
            },
        );
        let error = recorder
            .request(Method::GET, &url, vec![], None)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("fixture"));
        assert_eq!(receipts(fixture.path()).len(), 1);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_zero_hedge_delay_turns_hedging_off() {
        let dir = tempfile::tempdir().unwrap();
        let recorder = hedging(dir.path(), hedge_after(0));
        let (url, count, _) = scripted_server(vec![(after(500), OK)]).await;
        recorder
            .request(Method::GET, &url, vec![], None)
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_run_sends_at_most_sixteen_hedges() {
        let dir = tempfile::tempdir().unwrap();
        let recorder = hedging(dir.path(), hedge_after(100));
        recorder
            .load
            .hedges
            .store(HEDGES_PER_RUN - 1, Ordering::SeqCst);
        let (url, count, _) =
            scripted_server(vec![(after(400), OK), (after(0), OK), (after(400), OK)]).await;
        // The sixteenth hedge is sent and wins.
        let started = Instant::now();
        recorder
            .request(Method::GET, &url, vec![], None)
            .await
            .unwrap();
        assert!(started.elapsed() < Duration::from_millis(350));
        // The seventeenth request waits for its original.
        let started = Instant::now();
        recorder
            .request(Method::GET, &url, vec![], None)
            .await
            .unwrap();
        assert!(started.elapsed() >= Duration::from_millis(400));
        assert_eq!(count.load(Ordering::SeqCst), 3);
        assert_eq!(recorder.load.hedges.load(Ordering::SeqCst), HEDGES_PER_RUN);
        let latency = &recorder.load_summary()["source_latency"]["127.0.0.1"];
        assert_eq!(latency["hedged"], 1);
    }

    #[tokio::test]
    async fn a_hedge_waits_for_a_hedge_permit_and_never_for_the_originals_permits() {
        let dir = tempfile::tempdir().unwrap();
        let recorder = hedging(dir.path(), hedge_after(200));
        // Three stalled originals, then two hedges: one answers late, one never.
        let (url, count, _) = scripted_server(vec![
            (NEVER, OK),
            (NEVER, OK),
            (NEVER, OK),
            (after(600), OK),
            (NEVER, OK),
        ])
        .await;
        let started = Instant::now();
        let requests = (0..3).map(|_| async {
            let result = tokio::time::timeout(
                Duration::from_secs(2),
                recorder.request(Method::GET, &url, vec![], None),
            )
            .await;
            (result.is_ok_and(|r| r.is_ok()), started.elapsed())
        });
        let probe = async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            count.load(Ordering::SeqCst)
        };
        let (results, sent_at_probe) = tokio::join!(futures::future::join_all(requests), probe);
        assert_eq!(sent_at_probe, 5, "two hedges in flight, the third waiting");
        let answered: Vec<Duration> = results
            .iter()
            .filter(|(ok, _)| *ok)
            .map(|(_, at)| *at)
            .collect();
        assert_eq!(answered.len(), 2);
        // The waiting hedge was sent once the late hedge freed its permit.
        assert!(answered.iter().all(|at| *at >= Duration::from_millis(750)));
        assert_eq!(count.load(Ordering::SeqCst), 6);
        let latency = &recorder.load_summary()["source_latency"]["127.0.0.1"];
        assert_eq!(latency["hedged"], 3);
        assert_eq!(latency["hedge_wins"], 2);
    }

    #[tokio::test]
    async fn a_429_on_the_hedge_closes_the_gate_and_the_original_decides() {
        let dir = tempfile::tempdir().unwrap();
        let recorder = hedging(dir.path(), hedge_after(200));
        let refused =
            "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 30\r\nContent-Length: 0\r\n\r\n";
        let (url, count, _) = scripted_server(vec![(after(600), OK), (after(0), refused)]).await;
        let response = recorder
            .request(Method::GET, &url, vec![], None)
            .await
            .unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.artifact, "raw/000000.body.gz");
        let error = recorder
            .request(Method::GET, &url, vec![], None)
            .await
            .unwrap_err();
        assert!(error.downcast_ref::<SourceRateLimited>().is_some());
        assert_eq!(count.load(Ordering::SeqCst), 2);
        let load = recorder.load_summary();
        assert_eq!(load["source_rate_limited_requests"], 2);
        assert_eq!(load["source_latency"]["127.0.0.1"]["hedge_wins"], 0);
    }

    #[tokio::test]
    async fn a_server_error_on_the_original_lets_a_sent_hedge_decide() {
        let dir = tempfile::tempdir().unwrap();
        let recorder = hedging(dir.path(), hedge_after(200));
        let failed = "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n";
        let (url, _, _) = scripted_server(vec![(after(400), failed), (after(400), OK)]).await;
        let response = recorder
            .request(Method::GET, &url, vec![], None)
            .await
            .unwrap();
        assert_eq!(response.status, 200);
        // Without a hedge in flight, a server error returns at once for the caller's retry.
        let (url, count, _) = scripted_server(vec![(after(50), failed)]).await;
        let response = recorder
            .request(Method::GET, &url, vec![], None)
            .await
            .unwrap();
        assert_eq!(response.status, 503);
        assert_eq!(count.load(Ordering::SeqCst), 1);
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

    /// Answers every connection with `reply` after `delay`, or closes it without an answer when
    /// `reply` is empty. Returns the address and every request it read.
    async fn scripted(
        reply: &'static [u8],
        delay: Duration,
    ) -> (std::net::SocketAddr, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let requests = seen.clone();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let requests = requests.clone();
                tokio::spawn(async move {
                    let mut request = vec![0; 4096];
                    let n = socket.read(&mut request).await.unwrap_or(0);
                    requests
                        .lock()
                        .unwrap()
                        .push(String::from_utf8_lossy(&request[..n]).into_owned());
                    tokio::time::sleep(delay).await;
                    let _ = socket.write_all(reply).await;
                });
            }
        });
        (addr, seen)
    }

    /// The cause class of a failed POST that carries a credential, which the error never shows,
    /// and whether the request was unsent.
    async fn cause_of(recorder: &HttpRecorder, url: &str) -> (TransportCause, bool) {
        let error = recorder
            .request_body_recorded_elsewhere(
                Method::POST,
                url,
                vec![("authorization".into(), "Bearer fernlet-secret".into())],
                Some(json!({"question":"fernlet"})),
                SendGate::default(),
            )
            .await
            .unwrap_err();
        let text = format!("{error:#}");
        assert!(!text.contains("fernlet-secret") && !text.contains("fernlet.test"));
        let failure = error
            .downcast_ref::<TransportFailure>()
            .unwrap_or_else(|| panic!("not a transport failure: {text}"));
        (failure.cause, failure.unsent)
    }

    #[tokio::test]
    async fn each_transport_failure_names_its_class_and_whether_it_was_sent() {
        let dir = tempfile::tempdir().unwrap();
        let config = RunConfig {
            timeout_secs: 1,
            ..RunConfig::default()
        };
        let recorder = HttpRecorder::loopback_for_test(dir.path(), &config).unwrap();
        // Nothing listens on a port that was just released.
        let closed = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        assert_eq!(
            cause_of(&recorder, &format!("http://{closed}/")).await,
            (TransportCause::ConnectRefused, true)
        );
        // The server reads the request, then closes or stays silent: the request was sent.
        let (hangs_up, seen) = scripted(b"", Duration::ZERO).await;
        assert_eq!(
            cause_of(&recorder, &format!("http://{hangs_up}/")).await,
            (TransportCause::Closed, false)
        );
        assert!(seen.lock().unwrap()[0].starts_with("POST / HTTP/1.1"));
        let (silent, _) = scripted(b"", Duration::from_secs(5)).await;
        assert_eq!(
            cause_of(&recorder, &format!("http://{silent}/")).await,
            (TransportCause::Timeout, false)
        );
        // Plain text where a TLS handshake is expected.
        let (plain, seen) = scripted(b"HTTP/1.1 200 OK\r\n\r\n", Duration::ZERO).await;
        assert_eq!(
            cause_of(&recorder, &format!("https://{plain}/")).await,
            (TransportCause::Tls, true)
        );
        assert!(!seen.lock().unwrap().concat().contains("POST"));
        let no_dns = recorder.without_dns_for_test().unwrap();
        assert_eq!(
            cause_of(&no_dns, "http://fernlet.test/").await,
            (TransportCause::Dns, true)
        );
        // A proxy that refuses the tunnel, as a restricted network does.
        let (proxy, seen) = scripted(
            b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n",
            Duration::ZERO,
        )
        .await;
        let denied = recorder
            .through_proxy_for_test(&format!("http://{proxy}"))
            .unwrap();
        assert_eq!(
            cause_of(&denied, "https://fernlet.test/").await,
            (TransportCause::ConnectDenied, true)
        );
        let tunnel = seen.lock().unwrap().concat();
        assert!(
            tunnel.starts_with("CONNECT fernlet.test:443 HTTP/1.1"),
            "{tunnel}"
        );
        assert!(!tunnel.contains("POST") && !tunnel.contains("fernlet-secret"));
    }

    #[tokio::test]
    async fn the_probe_sends_one_bare_head_and_any_status_passes() {
        let dir = tempfile::tempdir().unwrap();
        let recorder = HttpRecorder::loopback_for_test(dir.path(), &RunConfig::default()).unwrap();
        let (answers, seen) = scripted(
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n",
            Duration::ZERO,
        )
        .await;
        assert_eq!(
            recorder.probe(&format!("http://{answers}/")).await,
            Ok(Some(404))
        );
        let request = seen.lock().unwrap().join("").to_ascii_lowercase();
        assert!(request.starts_with("head / http/1.1\r\n"), "{request}");
        assert!(!request.contains("authorization") && !request.contains("content-length"));
        // A connection with a slow answer shows a working path.
        let (slow, _) = scripted(b"", PROBE_WAIT * 2).await;
        assert_eq!(recorder.probe(&format!("http://{slow}/")).await, Ok(None));
        // A fixture recorder never sends.
        let fixture = HttpRecorder::new(
            dir.path(),
            &RunConfig {
                fixture: true,
                ..RunConfig::default()
            },
        )
        .unwrap();
        assert_eq!(
            fixture.probe(&format!("http://{answers}/")).await,
            Err(TransportCause::Other)
        );
        assert_eq!(seen.lock().unwrap().len(), 1);
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
        for public in [
            "93.184.216.34",
            "2606:4700::1111",
            "::ffff:93.184.216.34",
            "64:ff9b::5db8:d822",
        ] {
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
            "64:ff9b:1::a00:1",
            "2002:a00:1::1",
            "2002:5db8:d822::1",
            "2001:0:4136:e378:8000:63bf:3fff:fdd2",
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
    async fn public_reads_count_in_the_run_host_load() {
        let dir = tempfile::tempdir().unwrap();
        let (port, _, _) =
            page_server(reply("200 OK\r\nContent-Type: text/plain", b"ok"), None).await;
        let (redirect_port, _, _) = page_server(
            reply(
                "302 Found\r\nLocation: /elsewhere\r\nContent-Type: text/plain",
                b"",
            ),
            None,
        )
        .await;
        let base = HttpRecorder::new(dir.path(), &RunConfig::default()).unwrap();
        let dns = |port| {
            let mut dns = TestDns {
                loopback: true,
                port: Some(port),
                ..TestDns::default()
            };
            dns.answers
                .insert("fernlet.test".into(), vec!["127.0.0.1".parse().unwrap()]);
            dns
        };
        base.public_reader_for_test(dns(port))
            .unwrap()
            .get_public("https://fernlet.test/page")
            .await
            .unwrap();
        base.public_reader_for_test(dns(redirect_port))
            .unwrap()
            .get_public("https://fernlet.test/page")
            .await
            .unwrap_err();
        // The reader shares the run's counters, so the run's load shows both reads.
        let load = base.load_summary();
        assert_eq!(load["source_requests"]["fernlet.test"], 2);
        let latency = &load["source_latency"]["fernlet.test"];
        assert_eq!(latency["completed"], 1);
        assert_eq!(latency["not_completed"], 1);
        assert_eq!(latency["peak_in_flight"], 1);
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
