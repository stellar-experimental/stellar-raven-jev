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

/// Lowercase hex SHA-256 of `data`.
pub fn sha256_hex(data: impl AsRef<[u8]>) -> String {
    hex(&Sha256::digest(data))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

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
pub(crate) const HEDGES_PER_HOST: usize = 2;
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
    /// Source requests cancelled while they still waited for a host permit in this client, per
    /// host. A deadline cut of such a request is client queue time, not source time.
    cancelled_while_queued: Mutex<std::collections::BTreeMap<String, u64>>,
}

/// One source request waiting for its host permit. Dropping it before `sent`, also by
/// cancellation, counts the request as cancelled while queued.
struct Queued<'a> {
    load: &'a LoadCounters,
    host: String,
    waiting: bool,
}

impl<'a> Queued<'a> {
    fn start(load: &'a LoadCounters, host: &str) -> Self {
        Self {
            load,
            host: host.to_owned(),
            waiting: true,
        }
    }

    /// The request left the queue: it holds its permit, or it ended for a reason of its own.
    fn left(&mut self) {
        self.waiting = false;
    }
}

impl Drop for Queued<'_> {
    fn drop(&mut self) {
        if self.waiting {
            *self
                .load
                .cancelled_while_queued
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .entry(self.host.clone())
                .or_default() += 1;
        }
    }
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

/// The Retry-After in a response: seconds, or an HTTP-date (a past date is zero). It is capped at
/// the longest gate closure before conversion, so no header can overflow a duration.
pub(crate) fn retry_after(
    headers: &std::collections::BTreeMap<String, String>,
) -> Option<Duration> {
    let value = headers.get("retry-after")?.trim();
    let seconds = value
        .parse::<f64>()
        .ok()
        .filter(|v| v.is_finite() && *v >= 0.0)
        .or_else(|| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            Some((http_date_seconds(value)? as f64 - now.as_secs_f64()).max(0.0))
        })?;
    Some(Duration::from_secs_f64(
        seconds.min(GATE_MAX_CLOSE.as_secs_f64()),
    ))
}

/// The rate-limit signals in a source response. Retry-After is seconds or an HTTP-date. A reset
/// above 10^9 is a Unix time in seconds; a smaller one is seconds from now. Other values are
/// ignored, and every duration is capped before conversion, so no header can overflow one.
/// A window that the source scopes to one serving instance (`x-ratelimit-scope: instance`) does
/// not describe this host's budget, so only its status and Retry-After are kept.
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
    let instance_scoped = headers
        .get("x-ratelimit-scope")
        .is_some_and(|scope| scope.trim().eq_ignore_ascii_case("instance"));
    let number = |name: &str| if instance_scoped { None } else { number(name) };
    crate::governor::SourceSignals {
        status,
        retry_after: retry_after(headers),
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
            self.metadata["body_sha256"] = json!(sha256_hex(&bytes));
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
    /// sent that did not complete (failed, cut, or a cancelled hedge race loser), requests
    /// cancelled before they were sent while they waited for a host permit, the most requests in
    /// flight at once, hedges sent, and hedges whose response was returned.
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
        let queued = self
            .load
            .cancelled_while_queued
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let mut hosts = sent;
        for host in queued.keys() {
            hosts.entry(host.clone()).or_default();
        }
        hosts
            .iter()
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
                        "cancelled_while_queued": queued.get(host).copied().unwrap_or(0),
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
        let key = format!("{url}\n{}", hex(&digest.finalize()));
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
                record.metadata["request_body_sha256"] = json!(sha256_hex(&bytes));
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
            let host = parsed.host_str().unwrap_or_default().to_owned();
            // Source requests and public reads count per host; Jev requests keep their own usage.
            let accounted = self.gates.is_some() || self.public.is_some();
            let mut queued = accounted.then(|| Queued::start(&self.load, &host));
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
                    if let Some(queued) = queued.as_mut() {
                        queued.left();
                    }
                    self.load.rate_limited.fetch_add(1, Ordering::Relaxed);
                    return Err(SourceRateLimited { scope, wait }.into());
                }
                waited += wait;
                self.load
                    .gate_wait_ms
                    .fetch_add(wait.as_millis() as u64, Ordering::Relaxed);
                tokio::time::sleep(wait).await;
            };
            if let Some(queued) = queued.as_mut() {
                queued.left();
            }
            if gate.stop.is_some_and(|stop| stop()) {
                return Err(NotSent.into());
            }
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
                            | "x-ratelimit-scope"
                            | "cf-ray"
                            | "server-timing"
                            | "x-vercel-id"
                            | "x-scout-match-mode"
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
mod tests;
