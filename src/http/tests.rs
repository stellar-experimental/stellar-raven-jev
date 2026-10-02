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
    for config in [RunConfig::default(), hedge_after(0)] {
        let dir = tempfile::tempdir().unwrap();
        let recorder = hedging(dir.path(), config);
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
async fn an_in_share_hedge_wins_without_raising_requests_in_flight() {
    let dir = tempfile::tempdir().unwrap();
    // Source hedging is off; the in-share hedge still applies.
    let recorder = hedging(
        dir.path(),
        RunConfig {
            concurrency: 2,
            ..RunConfig::default()
        },
    );
    let (url, count, _) = scripted_server(vec![(NEVER, OK), (after(50), OK)]).await;
    let started = Instant::now();
    let response = recorder
        .request_hedged_in_share(&url, vec![], Duration::from_millis(200))
        .await
        .unwrap();
    assert_eq!(response.status, 200);
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(count.load(Ordering::SeqCst), 2);
    let latency = &recorder.load_summary()["source_latency"]["127.0.0.1"];
    assert_eq!(latency["hedged"], 1);
    assert_eq!(latency["hedge_wins"], 1);
    assert!(latency["peak_in_flight"].as_u64().unwrap() <= 2);
}

#[tokio::test]
async fn an_in_share_hedge_waits_for_the_hosts_own_permit() {
    let dir = tempfile::tempdir().unwrap();
    let recorder = hedging(
        dir.path(),
        RunConfig {
            concurrency: 1,
            ..RunConfig::default()
        },
    );
    let (url, count, _) = scripted_server(vec![(NEVER, OK), (after(0), OK)]).await;
    let result = tokio::time::timeout(
        Duration::from_millis(800),
        recorder.request_hedged_in_share(&url, vec![], Duration::from_millis(100)),
    )
    .await;
    assert!(
        result.is_err(),
        "the stalled original holds the only permit"
    );
    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "no second request in flight"
    );
}

#[tokio::test]
async fn a_429_on_the_hedge_closes_the_gate_and_the_original_decides() {
    let dir = tempfile::tempdir().unwrap();
    let recorder = hedging(dir.path(), hedge_after(200));
    let refused = "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 30\r\nContent-Length: 0\r\n\r\n";
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

#[tokio::test]
async fn a_request_cut_while_it_waits_for_a_host_permit_counts_as_queued_not_sent() {
    let dir = tempfile::tempdir().unwrap();
    let config = RunConfig {
        concurrency: 1,
        ..RunConfig::default()
    };
    let mut recorder = HttpRecorder::new(dir.path(), &config).unwrap();
    recorder.allow_loopback = true;
    let recorder = recorder.with_source_gates(Arc::new(crate::governor::Governor::local()));
    let (url, count) = counting_server(OK, Duration::from_millis(300)).await;
    let search = format!("{url}/api/find");
    let first = recorder.request(Method::GET, &search, vec![], None);
    let second = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        tokio::time::timeout(
            Duration::from_millis(50),
            recorder.request(Method::GET, &search, vec![], None),
        )
        .await
    };
    let (first, second) = tokio::join!(first, second);
    first.unwrap();
    assert!(second.is_err(), "the second request is cut while it waits");
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let latency = &recorder.load_summary()["source_latency"]["127.0.0.1"];
    assert_eq!(latency["completed"], 1);
    assert_eq!(latency["not_completed"], 0);
    assert_eq!(latency["cancelled_while_queued"], 1);
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

#[test]
fn an_instance_scoped_window_is_not_learned_but_its_refusal_is_kept() {
    let headers = |pairs: &[(&str, &str)]| {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    let window = [
        ("x-ratelimit-limit", "1200"),
        ("x-ratelimit-remaining", "1195"),
        ("x-ratelimit-reset", "60"),
    ];
    let host = source_signals(200, &headers(&window));
    assert_eq!(host.limit, Some(1200));
    let mut scoped = window.to_vec();
    scoped.push(("x-ratelimit-scope", "Instance"));
    let instance = source_signals(200, &headers(&scoped));
    assert_eq!(
        (instance.limit, instance.remaining, instance.reset_ms),
        (None, None, None)
    );
    scoped.push(("retry-after", "2"));
    let refused = source_signals(429, &headers(&scoped));
    assert_eq!(refused.retry_after, Some(Duration::from_secs(2)));
    assert_eq!(refused.limit, None);
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
    let metadata: Value =
        serde_json::from_slice(&std::fs::read(directory.path().join("raw/000000.json")).unwrap())
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
    let metadata: Value =
        serde_json::from_slice(&std::fs::read(directory.path().join("raw/000000.json")).unwrap())
            .unwrap();
    assert_eq!(metadata["body_artifact"], "raw/000000.body.gz");
    assert_eq!(metadata["body_encoding"], "gzip");
    assert_eq!(metadata["complete"], false);
    assert!(metadata["failure"].as_str().unwrap().contains("cancelled"));
    let body = raw_body(&directory.path().join("raw/000000.body.gz"));
    assert_eq!(body, b"abc");
    assert_eq!(metadata["body_sha256"], sha256_hex(&body));
    assert!(!streamed.exists(), "the uncompressed copy is removed");
    server.abort();
}
#[tokio::test]
async fn preserves_retry_after_ms_and_filters_unsafe_response_headers() {
    let dir = tempfile::tempdir().unwrap();
    let mut recorder = HttpRecorder::new(dir.path(), &RunConfig::default()).unwrap();
    recorder.allow_loopback = true;
    let url = server("HTTP/1.1 429 Too Many Requests\r\nRetry-After-Ms: 250\r\nRetry-After: 2\r\nServer-Timing: total;dur=5\r\nX-Vercel-Id: iad1::abc-1\r\nSet-Cookie: private=value\r\nX-Unknown-Secret: private\r\nContent-Length: 4\r\n\r\nwait").await;
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
    assert_eq!(
        response.headers.get("server-timing").map(String::as_str),
        Some("total;dur=5")
    );
    assert_eq!(
        response.headers.get("x-vercel-id").map(String::as_str),
        Some("iad1::abc-1")
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
    let url = server(
        "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/leak\r\nContent-Length: 4\r\n\r\nmove",
    )
    .await;
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
    let (port, _, _) = page_server(reply("200 OK\r\nContent-Type: text/plain", &body), None).await;
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
    let (port, _, _) = page_server(reply("200 OK\r\nContent-Type: text/plain", b"ok"), None).await;
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

#[test]
fn sha256_hex_is_lowercase_hex_of_the_digest() {
    assert_eq!(
        sha256_hex(b"abc"),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert_eq!(sha256_hex(b"").len(), 64);
}
