pub mod algolia;
pub mod lumenloop;
pub mod mintlify;
pub mod original;
pub(crate) mod published;
pub mod stellarlight;

use crate::types::*;
use anyhow::{bail, Result};

/// Retries of one source read after a server error or a failed response.
pub(crate) const READ_RETRIES: u32 = 2;
/// Source reads that one run may send again, across every source.
pub(crate) const READ_RETRIES_PER_RUN: u64 = 64;
/// The wait before the first retry. Each later retry doubles it, and up to the same again of
/// random spread is added, so that concurrent readers do not retry together.
const READ_RETRY_FIRST: std::time::Duration = std::time::Duration::from_millis(250);
/// The longest Retry-After that a retry waits out. A longer one would spend most of the fetch
/// deadline, so the failure is reported instead.
pub(crate) const RETRY_AFTER_LIMIT: std::time::Duration = std::time::Duration::from_secs(4);
/// The least time left for the retried request itself after its wait, before the fetch deadline.
/// A retry also needs as long as the attempt before it took, because a connector still running at
/// the deadline loses every document.
pub(crate) const RETRY_ROOM: std::time::Duration = std::time::Duration::from_secs(1);
/// A host with this many server errors or failed responses in a row gets no retries until it
/// answers again. A retry then would only add load to a host that is down.
const HOST_FAILURES_WITHOUT_RETRY: u32 = 4;

/// The last outcome of a source read, and a note for a report when an attempt failed: the earlier
/// attempts, and why a failure was not sent again.
pub(crate) struct Read {
    pub response: Result<crate::http::HttpResponse>,
    pub note: Option<String>,
}

/// Whether a read outcome may be sent again: HTTP 500, 502, or 503, or a failure before a
/// complete response, unless it was a timeout or a 4xx status had arrived. HTTP 504 is not
/// repeated: an upstream that sends it has already spent its time limit, and so has a timeout.
/// A refusal by a rate limit is not repeated either; its gate decides when to send again.
fn read_retryable(outcome: &Result<crate::http::HttpResponse>) -> bool {
    match outcome {
        Ok(response) => matches!(response.status, 500 | 502 | 503),
        Err(error) => error
            .downcast_ref::<crate::http::TransportFailure>()
            .is_some_and(|failure| {
                failure.cause != crate::http::TransportCause::Timeout
                    && !failure
                        .status
                        .is_some_and(|status| (400..500).contains(&status))
            }),
    }
}

fn read_outcome_text(outcome: &Result<crate::http::HttpResponse>) -> String {
    match outcome {
        Ok(response) => format!("HTTP {}", response.status),
        Err(error) => error.to_string().trim_end_matches('.').to_owned(),
    }
}

/// Send one source read, and send it again after a server error or a failed response (see
/// [`read_retryable`]), at most [`READ_RETRIES`] times. Each retry waits for the Retry-After the
/// host sent, or else for the exponential backoff, and starts only when it still leaves
/// [`RETRY_ROOM`] before the fetch deadline, while its host has not failed
/// [`HOST_FAILURES_WITHOUT_RETRY`] reads in a row, and while the run has retries left. Every
/// source read is a read, so a repeat is safe. A GET without a body is hedged like any source GET.
pub(crate) async fn read(
    ctx: &FetchContext,
    method: reqwest::Method,
    url: &str,
    headers: Vec<(String, String)>,
    body: Option<serde_json::Value>,
) -> Read {
    // One id names the first attempt of a GET (and its hedge), so a retry can name it as its
    // parent even when that attempt failed before a response.
    let race = (method == reqwest::Method::GET && body.is_none())
        .then(|| uuid::Uuid::new_v4().to_string());
    let mut sent = tokio::time::Instant::now();
    let mut outcome = match &race {
        Some(race) => {
            ctx.http
                .request_attempt(url, headers.clone(), race.clone())
                .await
        }
        None => {
            ctx.http
                .request(method.clone(), url, headers.clone(), body.clone())
                .await
        }
    };
    let mut earlier: Vec<String> = Vec::new();
    let mut stopped = None;
    loop {
        let failed = read_retryable(&outcome);
        let failures = ctx.http.note_source_outcome(url, failed);
        if !failed || earlier.len() as u32 == READ_RETRIES {
            break;
        }
        let asked = outcome
            .as_ref()
            .ok()
            .and_then(|response| crate::http::retry_after(&response.headers));
        let base = READ_RETRY_FIRST * (1u32 << earlier.len());
        let spread = std::time::Duration::from_millis(
            (uuid::Uuid::new_v4().as_u128() % (base.as_millis() + 1)) as u64,
        );
        let delay = asked.unwrap_or_default().max(base) + spread;
        let room = sent.elapsed().max(RETRY_ROOM);
        let reason = if let Some(asked) = asked.filter(|asked| *asked > RETRY_AFTER_LIMIT) {
            Some(format!(
                "its Retry-After of {} s is above the {} s retry limit",
                asked.as_secs_f64(),
                RETRY_AFTER_LIMIT.as_secs()
            ))
        } else if failures >= HOST_FAILURES_WITHOUT_RETRY {
            Some(format!("its host failed {failures} reads in a row"))
        } else if ctx
            .deadline
            .is_some_and(|deadline| tokio::time::Instant::now() + delay + room > deadline)
        {
            Some(format!(
                "a retry after {} ms would end past the fetch deadline",
                delay.as_millis()
            ))
        } else if !ctx.http.take_source_retry(READ_RETRIES_PER_RUN) {
            Some("the run's source retries are spent".to_owned())
        } else {
            None
        };
        if let Some(reason) = reason {
            stopped = Some(reason);
            break;
        }
        earlier.push(read_outcome_text(&outcome));
        let parent = outcome
            .as_ref()
            .ok()
            .map(|response| response.artifact.clone());
        tokio::time::sleep(delay).await;
        sent = tokio::time::Instant::now();
        outcome = ctx
            .http
            .request_retry(
                method.clone(),
                url,
                headers.clone(),
                body.clone(),
                parent,
                race.clone(),
            )
            .await;
    }
    const ORDINALS: [&str; 2] = ["First", "Second"];
    let mut parts: Vec<String> = Vec::new();
    if let Some(reason) = stopped {
        let again = if earlier.is_empty() { "" } else { " again" };
        parts.push(format!("It was not retried{again}: {reason}."));
    }
    for (ordinal, text) in ORDINALS.iter().zip(&earlier) {
        parts.push(format!("{ordinal} attempt: {text}."));
    }
    Read {
        response: outcome,
        note: (!parts.is_empty()).then(|| parts.join(" ")),
    }
}

/// An explicit source scope, separate from the result serialization format.
#[derive(Clone, Copy, Debug, Default, clap::ValueEnum, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceScope {
    #[default]
    All,
    Agentic,
}

impl SourceScope {
    pub fn includes(self, source: &Source) -> bool {
        match self {
            Self::All => true,
            Self::Agentic => matches!(
                source.id.as_str(),
                "mintlify:docs"
                    | "stellarlight.skills"
                    | "stellarlight.repos"
                    | "stellarlight.contracts"
                    | "stellarlight.audits"
                    | "stellarlight.research.dev-docs"
                    | "stellarlight.research.repo-docs"
                    | "stellarlight.research.sep"
                    | "stellarlight.research.cap"
                    | "stellarlight.research.release"
                    | "stellarlight.research.audit"
            ),
        }
    }

    pub fn description(self) -> &'static str {
        match self {
            Self::All => "All registered sources can route. Retrieval remains bounded.",
            Self::Agentic => "Developer documentation, standards, repositories, skills, tools, contracts, releases, and audits. Other sources are excluded before routing. This source filter can miss useful articles or research. It does not verify resource safety, freshness, or completeness.",
        }
    }
}

pub fn sources() -> Vec<Source> {
    let mut result = lumenloop::sources();
    result.extend(stellarlight::sources());
    result.extend(mintlify::sources());
    result.extend(algolia::sources());
    result
}

/// The first request of each source, counted per request scope, so a question can book them in
/// any window the source advertises before it sends them.
pub fn first_requests<'a>(sources: impl IntoIterator<Item = &'a Source>) -> Vec<(String, u64)> {
    let mut demands = std::collections::BTreeMap::<String, u64>::new();
    for source in sources {
        let scope = match source.family.as_str() {
            "stellarlight" => stellarlight::first_request_scope(source),
            _ => None,
        };
        if let Some(scope) = scope {
            *demands.entry(scope).or_default() += 1;
        }
    }
    demands.into_iter().collect()
}

/// The host a source's search requests go to, when it is known before fetching.
pub fn search_host(source: &Source) -> Option<String> {
    match source.family.as_str() {
        "stellarlight" => stellarlight::host(),
        "lumenloop" => lumenloop::host(),
        "mintlify" => mintlify::search_host(),
        "algolia" => algolia::search_host(source),
        _ => None,
    }
}

/// The in-flight request limit that a source's operator states for one client host, by host.
fn stated_host_limits() -> Vec<(String, usize)> {
    stellarlight::host()
        .map(|host| (host, stellarlight::HOST_CONCURRENCY))
        .into_iter()
        .collect()
}

/// Request windows that source operators state, as (request scope, limit, window length). The
/// governor counts them from the first request, and holds any advertised window of the same scope
/// to the stated limit.
pub fn stated_windows() -> Vec<(String, u64, std::time::Duration)> {
    stellarlight::stated_windows()
}

/// The requests one question may have in flight on one host: its per-host concurrency, plus hedge
/// permits when hedging is on.
fn requests_per_question(config: &RunConfig) -> usize {
    let hedges = if config.source_hedge_ms > 0 {
        crate::http::HEDGES_PER_HOST
    } else {
        0
    };
    (config.concurrency + hedges).max(1)
}

/// Fetch slots per host: for each host with a stated limit, that limit divided by the requests
/// one question may have in flight on a host (its per-host concurrency, plus hedge permits when
/// hedging is on), and at least 1. Explicit `source_slots` replace these per host and add others.
pub fn source_slots(config: &RunConfig) -> std::collections::BTreeMap<String, usize> {
    let per_question = requests_per_question(config);
    let mut slots: std::collections::BTreeMap<String, usize> = stated_host_limits()
        .into_iter()
        .map(|(host, limit)| (host, (limit / per_question).max(1)))
        .collect();
    slots.extend(config.source_slots.clone());
    slots
}

/// Reject settings that let one question set more requests in flight on a host than its operator
/// states for one client host: fetch slots times the requests one question may have in flight there.
pub fn check_source_slots(config: &RunConfig) -> Result<()> {
    let per_question = requests_per_question(config);
    let slots = source_slots(config);
    for (host, limit) in stated_host_limits() {
        let host_slots = slots.get(&host).copied().unwrap_or(1);
        if host_slots * per_question > limit {
            bail!(
                "{host} states at most {limit} requests in flight for one client host; {host_slots} fetch slots of {per_question} requests each allow {}. Lower --source-slots or --concurrency",
                host_slots * per_question
            );
        }
    }
    Ok(())
}

/// The capped hosts that fetching `sources` uses, in a stable order, as (host, units, need). A
/// host with a stated in-flight limit has that many units, one per request in flight, and a
/// question needs the requests it may have in flight there, or more when explicit slots allow fewer
/// questions. A host with only explicit slots has one unit per slot, and a question needs one.
pub fn host_caps<'a>(
    sources: impl IntoIterator<Item = &'a Source>,
    config: &RunConfig,
) -> Vec<(String, usize, usize)> {
    let slots = source_slots(config);
    let stated: std::collections::BTreeMap<String, usize> =
        stated_host_limits().into_iter().collect();
    let per_question = requests_per_question(config);
    let hosts: std::collections::BTreeSet<String> =
        sources.into_iter().filter_map(search_host).collect();
    hosts
        .into_iter()
        .filter_map(|host| {
            let host_slots = *slots.get(&host)?;
            Some(match stated.get(&host) {
                Some(&limit) => {
                    // Explicit slots can only let fewer questions in at once.
                    let need = if config.source_slots.contains_key(&host) {
                        per_question.max(limit.div_ceil(host_slots.max(1)))
                    } else {
                        per_question
                    };
                    (host, limit, need.min(limit))
                }
                None => (host, host_slots, 1),
            })
        })
        .collect()
}

pub async fn fetch(ctx: &FetchContext, source: &Source, question: &str) -> Result<FetchResult> {
    match source.family.as_str() {
        "lumenloop" => lumenloop::fetch(ctx, source, question).await,
        "stellarlight" => stellarlight::fetch(ctx, source, question).await,
        "mintlify" => mintlify::fetch(ctx, source, question).await,
        "algolia" => algolia::fetch(ctx, source, question).await,
        family => bail!("Unknown connector family: {family}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stated_limits_give_default_slots_and_explicit_slots_replace_or_add() {
        let scout = stellarlight::host().unwrap();
        let limit = stellarlight::HOST_CONCURRENCY;
        let mut config = RunConfig::default();
        assert_eq!(source_slots(&config)[&scout], limit / config.concurrency);
        config.concurrency = limit * 2;
        assert_eq!(source_slots(&config)[&scout], 1);
        config.concurrency = 8;
        config.source_hedge_ms = 4_000;
        assert_eq!(
            source_slots(&config)[&scout],
            limit / (8 + crate::http::HEDGES_PER_HOST)
        );
        config.source_slots = std::collections::BTreeMap::from([
            (scout.clone(), 1_000),
            ("fernlet.test".to_owned(), 3),
        ]);
        let slots = source_slots(&config);
        assert_eq!(slots[&scout], 1_000);
        assert_eq!(slots["fernlet.test"], 3);
    }

    #[test]
    fn a_question_holds_its_own_in_flight_requests_unless_slots_say_fewer_questions() {
        let scout = stellarlight::host().unwrap();
        let limit = stellarlight::HOST_CONCURRENCY;
        let research: Vec<Source> = stellarlight::sources()
            .into_iter()
            .filter(|s| s.id.starts_with("stellarlight.research."))
            .take(1)
            .collect();
        let mut config = RunConfig {
            concurrency: 10,
            ..RunConfig::default()
        };
        assert_eq!(
            host_caps(research.iter(), &config),
            vec![(scout.clone(), limit, 10)]
        );
        config.source_slots = std::collections::BTreeMap::from([(scout.clone(), 1)]);
        assert_eq!(
            host_caps(research.iter(), &config),
            vec![(scout, limit, limit)]
        );
    }

    #[test]
    fn settings_above_a_stated_host_limit_are_rejected() {
        let scout = stellarlight::host().unwrap();
        let limit = stellarlight::HOST_CONCURRENCY;
        let mut config = RunConfig::default();
        assert!(check_source_slots(&config).is_ok());
        config.source_slots = std::collections::BTreeMap::from([(scout.clone(), 3)]);
        let error = check_source_slots(&config).unwrap_err().to_string();
        assert!(error.contains(&format!("at most {limit}")), "{error}");
        config.source_slots.clear();
        config.concurrency = limit + 1;
        assert!(check_source_slots(&config).is_err());
        config.concurrency = 8;
        config.source_slots = std::collections::BTreeMap::from([("fernlet.test".to_owned(), 40)]);
        assert!(
            check_source_slots(&config).is_ok(),
            "hosts without a stated limit are not checked"
        );
    }

    /// A loopback server that sends `replies` in order, then the last one forever, and counts
    /// the connections it accepts.
    async fn replying(
        replies: Vec<&'static [u8]>,
    ) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/read", listener.local_addr().unwrap());
        let accepted = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = accepted.clone();
        tokio::spawn(async move {
            let last = *replies.last().unwrap();
            for reply in replies.into_iter().chain(std::iter::repeat(last)) {
                let (mut socket, _) = listener.accept().await.unwrap();
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let mut request = vec![0; 4096];
                let _ = socket.read(&mut request).await;
                let _ = socket.write_all(reply).await;
            }
        });
        (url, accepted)
    }

    fn context(dir: &std::path::Path) -> FetchContext {
        let config = RunConfig::default();
        FetchContext {
            http: crate::http::HttpRecorder::loopback_for_test(dir, &config).unwrap(),
            config,
            deadline: None,
        }
    }

    const UNAVAILABLE: &[u8] = b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n";
    const BAD_GATEWAY: &[u8] = b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n";
    const OK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}";

    #[tokio::test]
    async fn a_read_is_sent_again_twice_and_the_note_names_each_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = context(dir.path());
        let (url, accepted) = replying(vec![UNAVAILABLE, BAD_GATEWAY, OK]).await;
        let started = std::time::Instant::now();
        let read = read(&ctx, reqwest::Method::GET, &url, vec![], None).await;
        assert_eq!(read.response.unwrap().status, 200);
        // The waits are 250 ms and 500 ms, each with spread.
        assert!(started.elapsed() >= std::time::Duration::from_millis(750));
        assert_eq!(accepted.load(std::sync::atomic::Ordering::SeqCst), 3);
        assert_eq!(
            read.note.as_deref(),
            Some("First attempt: HTTP 503. Second attempt: HTTP 502.")
        );
        assert_eq!(ctx.http.load_summary()["source_retried_requests"], 2);
    }

    #[tokio::test]
    async fn a_gateway_timeout_and_a_client_error_are_final() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = context(dir.path());
        for reply in [
            &b"HTTP/1.1 504 Gateway Timeout\r\nContent-Length: 0\r\n\r\n"[..],
            &b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n"[..],
            &b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\n\r\n"[..],
        ] {
            let (url, accepted) = replying(vec![reply, OK]).await;
            let read = read(
                &ctx,
                reqwest::Method::POST,
                &url,
                vec![],
                Some(serde_json::json!({})),
            )
            .await;
            assert!(read.response.unwrap().status >= 400);
            assert!(read.note.is_none());
            assert_eq!(accepted.load(std::sync::atomic::Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn a_host_that_keeps_failing_gets_no_retries_until_it_answers() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = context(dir.path());
        let (url, accepted) = replying(vec![UNAVAILABLE]).await;
        // Three failures, then the fourth in a row stops the retries.
        let first = read(&ctx, reqwest::Method::GET, &url, vec![], None).await;
        assert_eq!(accepted.load(std::sync::atomic::Ordering::SeqCst), 3);
        assert!(first.note.unwrap().starts_with("First attempt"));
        let second = read(&ctx, reqwest::Method::GET, &url, vec![], None).await;
        assert_eq!(accepted.load(std::sync::atomic::Ordering::SeqCst), 4);
        assert_eq!(
            second.note.as_deref(),
            Some("It was not retried: its host failed 4 reads in a row.")
        );
        // An answer resets the count.
        assert_eq!(ctx.http.note_source_outcome(&url, false), 0);
        assert_eq!(ctx.http.note_source_outcome(&url, true), 1);
    }

    #[tokio::test]
    async fn the_run_stops_retrying_when_its_source_retries_are_spent() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = context(dir.path());
        while ctx.http.take_source_retry(READ_RETRIES_PER_RUN) {}
        let (url, accepted) = replying(vec![UNAVAILABLE, OK]).await;
        let read = read(&ctx, reqwest::Method::GET, &url, vec![], None).await;
        assert_eq!(read.response.unwrap().status, 503);
        assert_eq!(accepted.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            read.note.as_deref(),
            Some("It was not retried: the run's source retries are spent.")
        );
        assert_eq!(
            ctx.http.load_summary()["source_retried_requests"],
            READ_RETRIES_PER_RUN
        );
    }

    #[tokio::test]
    async fn a_retry_needs_as_much_room_as_the_attempt_before_it_took() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        // The host answers each request after 1.5 s.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/read", listener.local_addr().unwrap());
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut request = vec![0; 4096];
                    let _ = socket.read(&mut request).await;
                    tokio::time::sleep(std::time::Duration::from_millis(1_500)).await;
                    let _ = socket.write_all(UNAVAILABLE).await;
                });
            }
        });
        let dir = tempfile::tempdir().unwrap();
        let mut ctx = context(dir.path());
        // After the first attempt, about 1.5 s remain: more than RETRY_ROOM after the wait, but
        // less than the 1.5 s the attempt took.
        ctx.deadline = Some(tokio::time::Instant::now() + std::time::Duration::from_millis(3_000));
        let read = read(&ctx, reqwest::Method::GET, &url, vec![], None).await;
        assert_eq!(read.response.unwrap().status, 503);
        let note = read.note.unwrap();
        assert!(note.contains("would end past the fetch deadline"), "{note}");
    }
}
