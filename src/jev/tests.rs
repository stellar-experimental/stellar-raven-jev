use super::*;

#[test]
fn cloudflare_backend_reads_its_settings_and_runs_each_request_once() {
    let env = |pairs: &'static [(&'static str, &'static str)]| {
        move |key: &str| {
            pairs
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.to_string())
        }
    };
    let no_profile = |_: &str| -> Result<String> { panic!("static token needs no profile") };
    const ACCOUNT: &str = "0123456789abcdef0123456789abcdef";
    let chain = providers_from(
        env(&[
            ("CLOUDFLARE_ACCOUNT_ID", ACCOUNT),
            ("CLOUDFLARE_API_TOKEN", "t"),
        ]),
        no_profile,
    )
    .unwrap();
    assert_eq!(chain.len(), 1);
    assert_eq!(chain[0].name(), "cloudflare");
    assert!(providers_from(env(&[("CLOUDFLARE_API_TOKEN", "t")]), no_profile).is_err());
    let (url, headers, body) = request_parts(
        &offline_cloudflare(),
        &json!({}),
        &Map::from_iter([("q".into(), json!({}))]),
    )
    .unwrap();
    assert!(url.starts_with("https://api.cloudflare.com/client/v4/accounts/"));
    assert!(headers.contains(&("cf-aig-max-attempts".into(), "1".into())));
    assert_eq!(body["model"], "typesafe/jev");
}

fn offline_cloudflare() -> Backend {
    Backend::Cloudflare {
        account: "0".repeat(32),
        token: "offline-placeholder".into(),
        gateway: "default".into(),
    }
}
use std::path::Path;
use std::sync::Arc;

#[test]
fn wrangler_auth_accepts_only_exact_oauth_json() {
    let valid = br#"{"type":"oauth","token":"fixture-secret"}"#;
    assert_eq!(parse_wrangler_oauth(valid).unwrap(), "fixture-secret");
    for invalid in [
        r#"{"type":"api_token","token":"fixture-secret"}"#,
        r#"{"type":"oauth","token":"fixture-secret","extra":true}"#,
        r#"{"type":"oauth","token":"fixture-secret","token":"other"}"#,
        r#"{"type":"oauth","type":"oauth","token":"fixture-secret"}"#,
        r#"{"type":"oauth","token":null}"#,
        r#"{"type":"oauth","token":42}"#,
        r#"{"type":"oauth","token":""}"#,
        r#"{"type":"oauth","token":"fixture-secret\n"}"#,
        r#"{"type":"oauth","token":"fixture secret"}"#,
        r#"{"type":"oauth","token":"é"}"#,
        r#"{"type":"oauth"}"#,
        r#"{"type":1,"token":"fixture-secret"}"#,
        r#"["fixture-secret"]"#,
        r#"fixture-secret"#,
        r#"{"type":"oauth","token":"fixture-secret"} trailing"#,
    ] {
        let error = parse_wrangler_oauth(invalid.as_bytes()).err().unwrap();
        assert!(!format!("{error:#}").contains("fixture-secret"));
    }
    assert!(parse_wrangler_oauth(&vec![b' '; AUTH_OUTPUT_LIMIT + 1]).is_err());
}

#[test]
fn wrangler_profile_resolution_preserves_static_tokens_and_fails_closed() {
    let token = cloudflare_token(
        Some("static-fixture".into()),
        Some("personal".into()),
        |_| panic!("Static credentials must not start Wrangler"),
    )
    .unwrap();
    assert_eq!(token, "static-fixture");
    assert!(cloudflare_token(None, None, |_| panic!("Missing profile must fail")).is_err());
    for invalid in [
        "",
        "--help",
        " personal",
        "personal\n",
        "../../personal",
        "personal;echo",
    ] {
        assert!(cloudflare_token(None, Some(invalid.into()), |_| {
            panic!("Invalid profile must not start Wrangler")
        })
        .is_err());
    }
    let token = cloudflare_token(None, Some("personal".into()), |profile| {
        assert_eq!(profile, "personal");
        Ok("oauth-fixture".into())
    })
    .unwrap();
    assert_eq!(token, "oauth-fixture");
    assert!(cloudflare_token(None, Some("personal".into()), |_| {
        bail!("Fixture authentication failure")
    })
    .is_err());
}

fn expected() -> BTreeSet<String> {
    ["a".into(), "b".into()].into()
}
fn response() -> Value {
    json!({"model":"jev-1.13.0","answers":{"a":{"type":"noul","noul":0.9},"b":{"type":"noul","noul":0.8}},"usage":{"input_tokens":100,"output_tokens":20}})
}
fn parse(value: Value) -> Result<ParsedResponse> {
    parse_response(&serde_json::to_vec(&value)?, &expected())
}
fn source() -> Source {
    Source {
        id: "docs".into(),
        name: "Official docs".into(),
        family: "algolia".into(),
        description: "Official developer documentation".into(),
    }
}
fn client(dir: &Path, budget: f64) -> JevClient {
    let config = RunConfig {
        fixture: true,
        output_dir: dir.to_path_buf(),
        budget_usd: budget,
        ..RunConfig::default()
    };
    let http = HttpRecorder::new(dir, &config).unwrap();
    let mut client = JevClient::with_settings(
        &config,
        &http,
        |_| None,
        |_| panic!("Test construction must not resolve credentials"),
    )
    .unwrap();
    // Tests swap in an offline provider; the network check must not reach a real one.
    client.network = tokio::sync::OnceCell::new_with(Some(NetworkCheck::default()));
    client
}

#[test]
fn independent_probabilities_need_not_sum_to_one() {
    let parsed = parse(response()).unwrap();
    assert!((parsed.answers.values().sum::<f64>() - 1.7).abs() < 1e-12);
}

#[test]
fn accepts_only_recognized_completed_envelopes() {
    let raw = response();
    assert!(
        parse(json!({"success":true,"errors":[],"result":{"state":"Completed","result":raw}}))
            .is_ok()
    );
    assert!(parse(json!({"state":"Running","result":response()})).is_err());
    assert!(parse(json!({"success":false,"result":response()})).is_err());
    assert!(parse(json!({"random":{"answers":response()}})).is_err());
    assert!(parse(json!({"success":true,"errors":[{"code":10000}],"result":response()})).is_err());
}

#[test]
fn rejects_missing_extra_mistyped_and_invalid_probabilities() {
    for invalid in [json!(-0.1), json!(1.1), json!("0.8"), Value::Null] {
        let mut raw = response();
        raw["answers"]["a"]["noul"] = invalid;
        assert!(parse(raw).is_err());
    }
    let mut raw = response();
    raw["answers"].as_object_mut().unwrap().remove("b");
    assert!(parse(raw).is_err());
    let mut raw = response();
    raw["answers"]["extra"] = json!({"type":"noul","noul":0.1});
    assert!(parse(raw).is_err());
    let mut raw = response();
    raw["answers"]["a"]["type"] = json!("score");
    assert!(parse(raw).is_err());
    let mut raw = response();
    raw["answers"]["a"]["confidence"] = json!(0.9);
    assert!(parse(raw).is_err());
    let mut raw = response();
    raw["usage"]["input_tokens"] = json!(-1);
    assert!(parse(raw).is_err());
    let mut raw = response();
    raw["usage"]
        .as_object_mut()
        .unwrap()
        .remove("output_tokens");
    assert!(parse(raw).is_err());
}

#[test]
fn rejects_duplicate_keys_before_they_are_overwritten() {
    for raw in [
        r#"{"model":"jev","answers":{"a":{"type":"noul","noul":0.2},"a":{"type":"noul","noul":0.9}},"usage":{"input_tokens":1,"output_tokens":1}}"#,
        r#"{"model":"jev","answers":{"a":{"type":"noul","noul":0.2,"noul":0.9}},"usage":{"input_tokens":1,"output_tokens":1}}"#,
        r#"{"model":"jev","answers":{"a":{"type":"noul","noul":NaN}},"usage":{"input_tokens":1,"output_tokens":1}}"#,
        r#"{"model":"jev","answers":{"a":{"type":"noul","noul":1e999}},"usage":{"input_tokens":1,"output_tokens":1}}"#,
    ] {
        assert!(parse_response(raw.as_bytes(), &expected()).is_err());
    }
}

#[test]
fn route_passes_submit_distinct_questions_and_explicit_cycles() {
    let first = source_question(&source(), 0);
    let second = source_question(&source(), 1);
    let dir = tempfile::tempdir().unwrap();
    let mut client = client(dir.path(), 0.0);
    client.backend = offline_cloudflare();
    let body0 = request_parts(
        &client.backend,
        &json!({"user_question":"q"}),
        &Map::from_iter([("q".into(), first.clone())]),
    )
    .unwrap()
    .2;
    let body1 = request_parts(
        &client.backend,
        &json!({"user_question":"q"}),
        &Map::from_iter([("q".into(), second.clone())]),
    )
    .unwrap()
    .2;
    assert_ne!(
        body0["input"]["questions"]["q"]["instructions"],
        body1["input"]["questions"]["q"]["instructions"]
    );
    assert!(body0["input"]["questions"]["q"]["instructions"].is_object());
    assert_ne!(first["instructions"], second["instructions"]);
    assert_ne!(first["criteria"], second["criteria"]);
    assert_eq!(first, source_question(&source(), 2));
    assert_eq!(route_lens(1), route_lens(3));
}

#[test]
fn chunks_cover_every_utf8_byte_including_the_tail() {
    let text = format!("{}TAIL-EVIDENCE", "é🙂文".repeat(4000));
    let chunks = text_chunks(&text, DOCUMENT_CHUNK_BYTES);
    assert!(chunks.len() > 1);
    let mut end = 0;
    let rebuilt: String = chunks
        .iter()
        .map(|(a, b)| {
            assert_eq!(*a, end);
            assert!(*b - *a <= DOCUMENT_CHUNK_BYTES);
            end = *b;
            &text[*a..*b]
        })
        .collect();
    assert_eq!(end, text.len());
    assert_eq!(rebuilt, text);
}

#[test]
fn reservations_stop_concurrent_overspending() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = client(dir.path(), 0.004);
    client.backend = offline_cloudflare();
    let client = Arc::new(client);
    let results: Vec<_> = (0..16)
        .map(|_| {
            let client = client.clone();
            std::thread::spawn(move || client.reserve().is_ok())
        })
        .collect();
    assert_eq!(
        results
            .into_iter()
            .map(|r| usize::from(r.join().unwrap()))
            .sum::<usize>(),
        1
    );
    assert_eq!(client.usage().requests, 1);
    assert!(client.usage().cost_usd <= 0.004);
}

#[tokio::test]
async fn a_full_budget_of_in_flight_reservations_makes_the_next_attempt_wait_not_fail() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = client(dir.path(), 0.004);
    client.backend = offline_cloudflare();
    let client = Arc::new(client);
    // One reservation fills the budget. With nothing in flight, the next one fails at once.
    let first = client.reserve_waiting().await.unwrap();
    let waiter = {
        let client = client.clone();
        tokio::spawn(async move { client.reserve_waiting().await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !waiter.is_finished(),
        "the second attempt waits for the first"
    );
    // A small real cost frees the budget; the waiter then reserves.
    client.settle(first, 1000, 10).unwrap();
    let second = tokio::time::timeout(std::time::Duration::from_secs(2), waiter)
        .await
        .expect("settlement wakes the waiter")
        .unwrap()
        .unwrap();
    assert_eq!(client.usage().requests, 2);
    client.settle(second, 1000, 10).unwrap();
    assert!(client.usage().cost_usd <= 0.004);
    // Nothing in flight and no room: fail immediately instead of waiting forever.
    let tiny = client_with_budget_nanos(dir.path(), 1);
    assert!(tiny.reserve_waiting().await.is_err());
}

#[tokio::test]
async fn a_stop_wakes_waiting_reservations_with_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = client(dir.path(), 0.004);
    client.backend = offline_cloudflare();
    let client = Arc::new(client);
    let _held = client.reserve_waiting().await.unwrap();
    let waiter = {
        let client = client.clone();
        tokio::spawn(async move { client.reserve_waiting().await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!waiter.is_finished());
    client.stop_on_unresolved_usage();
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), waiter)
        .await
        .expect("a stop wakes the waiter")
        .unwrap();
    assert!(result.is_err());
}

#[test]
fn unresolved_attempts_keep_reservations_and_open_the_circuit_after_three_in_a_row() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = client(dir.path(), 1.0);
    client.backend = offline_cloudflare();
    // Two unresolved attempts retain their reservations but do not stop the client.
    let first = client.reserve().unwrap();
    assert!(!client.retain_unresolved("timeout"));
    let second = client.reserve().unwrap();
    assert!(!client.retain_unresolved("timeout"));
    assert!(client.spending_stop_reason().is_none());
    // A settled attempt resets the run of failures.
    let ok = client.reserve().unwrap();
    client.settle(ok, 1000, 10).unwrap();
    for _ in 0..2 {
        client.reserve().unwrap();
        assert!(!client.retain_unresolved("http_500"));
    }
    // The third consecutive unresolved attempt opens the circuit.
    client.reserve().unwrap();
    assert!(client.retain_unresolved("connect_denied"));
    // A later call reserves nothing: its item is not assessed, which is not a failed judgment.
    let refused = client.reserve().unwrap_err();
    assert!(not_assessed_after_stop(&refused));
    assert_eq!(failure_cause(&refused), None);
    let message = refused.to_string();
    assert!(message.contains("unresolved paid-attempt usage"));
    assert!(message.contains("(last cause: connect_denied)"));
    assert!(message.contains("nothing was reserved for this call"));
    let ledger = client.ledger.lock().unwrap();
    let settled = client.backend.cost_nanos(1000).unwrap();
    assert_eq!(ledger.accounted_nanos, 5 * first.max(second) + settled);
    assert_eq!(ledger.in_flight, 0);
}

fn client_with_budget_nanos(dir: &Path, nanos: u64) -> JevClient {
    let mut client = client(dir, 0.0);
    client.backend = offline_cloudflare();
    client.ledger.lock().unwrap().budget_nanos = nanos;
    client
}

#[test]
fn failed_attempts_keep_reservations_and_successes_count_usage() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = client(dir.path(), 0.01);
    client.backend = offline_cloudflare();
    let failed = client.reserve().unwrap();
    let success = client.reserve().unwrap();
    client.settle(success, 1000, 100).unwrap();
    let usage = client.usage();
    assert_eq!(usage.requests, 2);
    assert_eq!(usage.input_tokens, 1000);
    assert_eq!(usage.output_tokens, 100);
    assert!((usage.cost_usd - (failed + 44100) as f64 / NANOS_PER_USD).abs() < 1e-12);
}

#[tokio::test]
async fn authentication_failure_blocks_later_calls_and_keeps_inflight_accounting() {
    for status in [401, 403] {
        let dir = tempfile::tempdir().unwrap();
        let mut client = client(dir.path(), 1.0);
        client.backend = offline_cloudflare();
        let failed = client.reserve().unwrap();
        let inflight = client.reserve().unwrap();
        assert!(client.stop_on_authentication_failure(status).unwrap());
        let client = Arc::new(client);
        let calls = (0..16).map(|_| {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .evaluate(
                        json!({}),
                        Map::from_iter([("a".into(), json!({"type":"noul"}))]),
                        json!({}),
                    )
                    .await
            })
        });
        for result in futures::future::join_all(calls).await {
            assert!(result
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("authentication failure"));
        }
        client.settle(inflight, 1000, 100).unwrap();
        assert_eq!(client.usage().requests, 2);
        assert_eq!(client.usage().input_tokens, 1000);
        assert_eq!(client.usage().output_tokens, 100);
        assert!((client.usage().cost_usd - (failed + 44100) as f64 / NANOS_PER_USD).abs() < 1e-12);
        assert!(std::fs::read_dir(dir.path().join("raw"))
            .unwrap()
            .next()
            .is_none());
        assert!(client.reserve().is_err());
    }
}

#[tokio::test]
async fn http_errors_stop_spending_and_preserve_raw_inflight_receipts() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;

    const TOKEN: &str = "offline-auth-circuit-token-7ea120";
    const ERROR_BODY: &str = r#"{"error":"fixture authentication rejected"}"#;

    async fn read_request(socket: &mut TcpStream) {
        let mut bytes = Vec::new();
        loop {
            let mut part = [0; 4096];
            let count = socket.read(&mut part).await.unwrap();
            assert!(count > 0, "The request ended before its body");
            bytes.extend_from_slice(&part[..count]);
            assert!(bytes.len() < MAX_REQUEST_BYTES + 4096);
            if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = std::str::from_utf8(&bytes[..end]).unwrap();
                let length: usize = head
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                assert!(head.contains(&format!("Bearer {TOKEN}")));
                if bytes.len() >= end + 4 + length {
                    return;
                }
            }
        }
    }

    async fn evaluate_fixture(client: Arc<JevClient>) -> Result<(BTreeMap<String, f64>, String)> {
        client
            .evaluate(
                json!({"fixture":"authentication circuit"}),
                Map::from_iter([
                    ("a".into(), json!({"type":"noul"})),
                    ("b".into(), json!({"type":"noul"})),
                ]),
                json!({"stage":"offline_loopback"}),
            )
            .await
    }

    for status in [401, 402, 403, 408, 503] {
        tokio::time::timeout(Duration::from_secs(10), async {
            let dir = tempfile::tempdir().unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let mut candidate = client(dir.path(), 1.0);
            candidate.http = HttpRecorder::loopback_for_test(
                dir.path(),
                &RunConfig {
                    fixture: false,
                    timeout_secs: 3,
                    concurrency: 4,
                    ..RunConfig::default()
                },
            )
            .unwrap();
            // The loopback URL reaches only this local server.
            candidate.backend = Backend::Loopback {
                url: format!("http://{address}/evaluate"),
                token: TOKEN.into(),
            };
            let reservation = candidate.backend.reservation().unwrap();
            // This test covers the circuit's mechanics, so it opens at the first unresolved attempt.
            candidate.ledger.lock().unwrap().unresolved_stop_after = 1;
            let client = Arc::new(candidate);
            let (first_seen, first_ready) = oneshot::channel();
            let (release, released) = oneshot::channel();
            let server = tokio::spawn(async move {
                let (mut first, _) = listener.accept().await.unwrap();
                read_request(&mut first).await;
                first_seen.send(()).unwrap();
                let (mut second, _) = listener.accept().await.unwrap();
                read_request(&mut second).await;
                let reply = format!(
                    "HTTP/1.1 {status} Rejected\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{ERROR_BODY}",
                    ERROR_BODY.len()
                );
                first.write_all(reply.as_bytes()).await.unwrap();
                first.shutdown().await.unwrap();
                released.await.unwrap();
                let body = response().to_string();
                let reply = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                second.write_all(reply.as_bytes()).await.unwrap();
                second.shutdown().await.unwrap();
                assert!(tokio::time::timeout(Duration::from_millis(50), listener.accept())
                    .await
                    .is_err(), "The open circuit sent another HTTP request");
            });
            let first = tokio::spawn(evaluate_fixture(client.clone()));
            first_ready.await.unwrap();
            let inflight = tokio::spawn(evaluate_fixture(client.clone()));
            let failure = first.await.unwrap().unwrap_err().to_string();
            assert!(failure.contains(&format!("HTTP {status}")));
            assert!(!failure.contains(TOKEN));
            assert_eq!(client.ledger.lock().unwrap().authentication_failed, matches!(status, 401 | 403));
            assert!(client.ledger.lock().unwrap().unresolved_usage);
            assert_eq!(client.usage().requests, 2);
            assert_eq!(client.ledger.lock().unwrap().accounted_nanos, 2 * reservation);
            assert_eq!(client.http.metrics()["requests_started"], 2);

            let later = (0..16).map(|_| tokio::spawn(evaluate_fixture(client.clone())));
            for result in futures::future::join_all(later).await {
                let error = result.unwrap().unwrap_err().to_string();
                assert!(error.contains(if matches!(status, 401 | 403) { "authentication failure" } else { "unresolved paid-attempt usage" }));
                assert!(!error.contains(TOKEN));
            }
            assert_eq!(client.usage().requests, 2);
            assert_eq!(client.ledger.lock().unwrap().accounted_nanos, 2 * reservation);
            assert_eq!(client.http.metrics()["requests_started"], 2);
            release.send(()).unwrap();
            let (answers, success_path) = inflight.await.unwrap().unwrap();
            assert_eq!(answers, BTreeMap::from([("a".into(), 0.9), ("b".into(), 0.8)]));
            server.await.unwrap();

            let settled = client.backend.cost_nanos(100).unwrap();
            let usage = client.usage();
            assert_eq!(usage.requests, 2);
            assert_eq!((usage.input_tokens, usage.output_tokens), (100, 20));
            assert_eq!(client.ledger.lock().unwrap().accounted_nanos, reservation + settled);
            assert!((usage.cost_usd - (reservation + settled) as f64 / NANOS_PER_USD).abs() < 1e-12);
            assert!(client.reserve().is_err());
            let success: Value = serde_json::from_slice(
                &std::fs::read(dir.path().join(success_path)).unwrap(),
            ).unwrap();
            assert_eq!(success["state"], "complete");
            let traces: Vec<Value> = std::fs::read_dir(dir.path().join("jev")).unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.file_name().unwrap().to_str().unwrap().contains("-attempt-"))
                .map(|path| serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap())
                .collect();
            assert_eq!(traces.len(), 2);
            let failed = traces.iter().find(|trace| trace["state"] == "http_error").unwrap();
            assert_eq!(failed["http_status"], status);
            assert_eq!(failed["authentication_circuit_open"] == true, matches!(status, 401 | 403));
            assert_eq!(failed["unresolved_usage_circuit_open"], true);
            assert_eq!(failed["reservation_usd"], reservation as f64 / NANOS_PER_USD);
            let artifact = failed["response_artifact"].as_str().unwrap();
            let mut body = Vec::new();
            std::io::Read::read_to_end(
                &mut flate2::read::GzDecoder::new(std::fs::File::open(dir.path().join(artifact)).unwrap()),
                &mut body,
            )
            .unwrap();
            assert_eq!(body, ERROR_BODY.as_bytes());
            let metadata: Value = serde_json::from_slice(
                &std::fs::read(dir.path().join(artifact.replace(".body.gz", ".json"))).unwrap(),
            ).unwrap();
            assert_eq!(metadata["status"], status);
            assert_eq!(metadata["complete"], true);
            assert_eq!(metadata["request_headers"]["authorization"], "[REDACTED]");
            assert_eq!(std::fs::read_dir(dir.path().join("raw")).unwrap().count(), 4);
            for folder in ["raw", "jev"] {
                for entry in std::fs::read_dir(dir.path().join(folder)).unwrap() {
                    let bytes = std::fs::read(entry.unwrap().path()).unwrap();
                    assert!(!String::from_utf8_lossy(&bytes).contains(TOKEN));
                }
            }
        })
        .await
        .expect("The offline authentication regression exceeded its deadline");
    }
}

#[test]
fn other_http_statuses_do_not_open_authentication_circuit() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = client(dir.path(), 1.0);
    client.backend = offline_cloudflare();
    for status in [200, 400, 404, 408, 429, 500, 503] {
        assert!(!client.stop_on_authentication_failure(status).unwrap());
    }
    assert!(client.reserve().is_ok());
}

#[tokio::test]
async fn one_upstream_block_page_does_not_stop_later_scoring_through_evaluate() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    const BLOCK: &str = r#"{"errors":[{"message":"Payment error from model using BYOK: <title>Attention Required! | Cloudflare</title>"}]}"#;
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/evaluate", listener.local_addr().unwrap());
    let client = JevClient::loopback_for_test(
        &RunConfig {
            output_dir: dir.path().to_path_buf(),
            budget_usd: 1.0,
            timeout_secs: 2,
            ..RunConfig::default()
        },
        url,
    )
    .unwrap();
    let reservation = client.backend.reservation().unwrap();
    let server = tokio::spawn(async move {
        for (status, body) in [
            ("402 Payment Required", BLOCK.to_string()),
            ("200 OK", response().to_string()),
        ] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut input = [0; 16384];
            assert!(socket.read(&mut input).await.unwrap() > 0);
            let reply = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(reply.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
        }
    });
    let questions = || {
        Map::from_iter([
            ("a".into(), json!({"type":"noul"})),
            ("b".into(), json!({"type":"noul"})),
        ])
    };
    let first = client
        .evaluate(json!({"fixture":1}), questions(), json!({}))
        .await;
    assert!(first.unwrap_err().to_string().contains("HTTP 402"));
    assert!(
        client.spending_stop_reason().is_none(),
        "one unresolved attempt must not stop the client"
    );
    let (answers, _) = client
        .evaluate(json!({"fixture":2}), questions(), json!({}))
        .await
        .unwrap();
    assert_eq!(answers.len(), 2);
    server.await.unwrap();
    let ledger = client.ledger.lock().unwrap();
    assert_eq!(ledger.in_flight, 0);
    assert_eq!(ledger.consecutive_unresolved, 0);
    let settled = client.backend.cost_nanos(100).unwrap();
    assert_eq!(
        ledger.accounted_nanos,
        reservation + settled,
        "the failed attempt keeps its full reservation"
    );
}

#[tokio::test]
async fn a_slow_call_gets_one_hedge_and_the_loser_is_charged_the_winner_input() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/evaluate", listener.local_addr().unwrap());
    let mut client = JevClient::loopback_for_test(
        &RunConfig {
            output_dir: dir.path().to_path_buf(),
            budget_usd: 1.0,
            timeout_secs: 5,
            full_record: true,
            ..RunConfig::default()
        },
        url,
    )
    .unwrap();
    client.hedge_after = Some(Duration::from_millis(100));
    // The first request stalls; the hedge answers at once.
    let server = tokio::spawn(async move {
        let mut sockets = Vec::new();
        for stall in [true, false] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut input = [0; 16384];
            assert!(socket.read(&mut input).await.unwrap() > 0);
            if stall {
                sockets.push(socket);
                continue;
            }
            let body = response().to_string();
            let reply = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(reply.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
        }
        sockets
    });
    let (answers, _) = client
        .evaluate(
            json!({"fixture":"hedge"}),
            Map::from_iter([
                ("a".into(), json!({"type":"noul"})),
                ("b".into(), json!({"type":"noul"})),
            ]),
            json!({}),
        )
        .await
        .unwrap();
    assert_eq!(answers.len(), 2);
    drop(server.await.unwrap());
    assert!(client.spending_stop_reason().is_none());
    let usage = client.usage();
    assert_eq!((usage.requests, usage.hedged_requests), (2, 1));
    assert_eq!(
        usage.input_tokens, 200,
        "the loser is charged the winner's input"
    );
    let ledger = client.ledger.lock().unwrap();
    assert_eq!(ledger.in_flight, 0);
    assert_eq!(
        ledger.accounted_nanos,
        2 * client.backend.cost_nanos(100).unwrap()
    );
    drop(ledger);
    let cancelled = std::fs::read_dir(dir.path().join("jev"))
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with("-cancelled.json")
        })
        .count();
    assert_eq!(cancelled, 1);
}

/// A loopback Jev server. Each entry answers one connection, in accept order, after its delay.
async fn scripted_server(
    replies: Vec<(u64, &'static str, String)>,
) -> (String, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/evaluate", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut tasks = Vec::new();
        for (delay, status, body) in replies {
            let (mut socket, _) = listener.accept().await.unwrap();
            tasks.push(tokio::spawn(async move {
                let mut input = [0; 16384];
                let _ = socket.read(&mut input).await;
                tokio::time::sleep(Duration::from_millis(delay)).await;
                let reply = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(reply.as_bytes()).await;
                let _ = socket.shutdown().await;
            }));
        }
        for task in tasks {
            let _ = task.await;
        }
    });
    (url, server)
}

fn hedging_client(dir: &Path, url: String, budget_usd: f64) -> JevClient {
    let mut client = JevClient::loopback_for_test(
        &RunConfig {
            output_dir: dir.to_path_buf(),
            budget_usd,
            timeout_secs: 5,
            ..RunConfig::default()
        },
        url,
    )
    .unwrap();
    client.hedge_after = Some(Duration::from_millis(100));
    client
}

#[tokio::test]
async fn a_failed_first_attempt_lets_the_running_hedge_decide() {
    let dir = tempfile::tempdir().unwrap();
    let (url, server) = scripted_server(vec![
        (300, "503 Service Unavailable", "{}".into()),
        (500, "200 OK", response().to_string()),
    ])
    .await;
    let client = hedging_client(dir.path(), url, 1.0);
    let reservation = client.backend.reservation().unwrap();
    let (answers, _) = client
        .evaluate(
            json!({"fixture":"hedge"}),
            Map::from_iter([
                ("a".into(), json!({"type":"noul"})),
                ("b".into(), json!({"type":"noul"})),
            ]),
            json!({}),
        )
        .await
        .unwrap();
    assert_eq!(answers.len(), 2);
    server.await.unwrap();
    assert!(client.spending_stop_reason().is_none());
    let usage = client.usage();
    assert_eq!((usage.requests, usage.hedged_requests), (2, 1));
    let ledger = client.ledger.lock().unwrap();
    assert_eq!((ledger.in_flight, ledger.consecutive_unresolved), (0, 0));
    assert_eq!(
        ledger.accounted_nanos,
        reservation + client.backend.cost_nanos(100).unwrap(),
        "the failed first attempt keeps its reservation; the hedge settles"
    );
}

#[tokio::test]
async fn a_rate_limited_call_releases_its_reservation_and_retries() {
    let dir = tempfile::tempdir().unwrap();
    let (url, server) = scripted_server(vec![
        (
            0,
            "429 Too Many Requests",
            "{\"errors\":[{\"code\":971}]}".into(),
        ),
        (0, "200 OK", response().to_string()),
    ])
    .await;
    let mut client = hedging_client(dir.path(), url, 1.0);
    client.hedge_after = None;
    client.rate_limit_wait = Some(Duration::from_millis(10));
    let (answers, _) = client
        .evaluate(
            json!({"fixture":"rate limit"}),
            Map::from_iter([
                ("a".into(), json!({"type":"noul"})),
                ("b".into(), json!({"type":"noul"})),
            ]),
            json!({}),
        )
        .await
        .unwrap();
    assert_eq!(answers.len(), 2);
    server.await.unwrap();
    let usage = client.usage();
    assert_eq!((usage.requests, usage.rate_limited_requests), (1, 1));
    assert!(client.spending_stop_reason().is_none());
    let ledger = client.ledger.lock().unwrap();
    assert_eq!((ledger.in_flight, ledger.consecutive_unresolved), (0, 0));
    assert_eq!(
        ledger.accounted_nanos,
        client.backend.cost_nanos(100).unwrap(),
        "the rejected request costs nothing"
    );
}

#[tokio::test]
async fn no_hedge_starts_while_the_first_attempt_waits_out_a_rate_limit() {
    let dir = tempfile::tempdir().unwrap();
    let (url, server) = scripted_server(vec![
        (0, "429 Too Many Requests", "{}".into()),
        (0, "200 OK", response().to_string()),
    ])
    .await;
    let mut client = hedging_client(dir.path(), url, 1.0);
    client.hedge_after = Some(Duration::from_millis(20));
    client.rate_limit_wait = Some(Duration::from_millis(300));
    let (answers, _) = client
        .evaluate(
            json!({"fixture":"rate limit with hedging"}),
            Map::from_iter([
                ("a".into(), json!({"type":"noul"})),
                ("b".into(), json!({"type":"noul"})),
            ]),
            json!({}),
        )
        .await
        .unwrap();
    assert_eq!(answers.len(), 2);
    server.await.unwrap();
    let usage = client.usage();
    assert_eq!(
        (
            usage.requests,
            usage.hedged_requests,
            usage.rate_limited_requests
        ),
        (1, 0, 1)
    );
}

#[tokio::test]
async fn a_hedge_queued_behind_a_rate_limited_request_is_never_sent() {
    let dir = tempfile::tempdir().unwrap();
    // The first request holds the only permit past the hedge delay, then gets 429.
    let (url, server) = scripted_server(vec![
        (200, "429 Too Many Requests", "{}".into()),
        (0, "200 OK", response().to_string()),
    ])
    .await;
    let mut client = hedging_client(dir.path(), url, 1.0);
    client.http = HttpRecorder::loopback_for_test(
        dir.path(),
        &RunConfig {
            fixture: false,
            timeout_secs: 5,
            concurrency: 1,
            ..RunConfig::default()
        },
    )
    .unwrap();
    client.hedge_after = Some(Duration::from_millis(20));
    client.rate_limit_wait = Some(Duration::from_millis(50));
    let (answers, _) = client
        .evaluate(
            json!({"fixture":"queued hedge"}),
            Map::from_iter([
                ("a".into(), json!({"type":"noul"})),
                ("b".into(), json!({"type":"noul"})),
            ]),
            json!({}),
        )
        .await
        .unwrap();
    assert_eq!(answers.len(), 2);
    server.await.unwrap();
    let usage = client.usage();
    assert_eq!(
        (
            usage.requests,
            usage.hedged_requests,
            usage.rate_limited_requests
        ),
        (1, 0, 1)
    );
    let ledger = client.ledger.lock().unwrap();
    assert_eq!((ledger.in_flight, ledger.consecutive_unresolved), (0, 0));
    assert!(!ledger.unresolved_usage && !ledger.stopped);
}

#[tokio::test]
async fn small_documents_share_one_scoring_call_and_keep_their_own_answers() {
    let dir = tempfile::tempdir().unwrap();
    let mut answers = serde_json::Map::new();
    for (k, usable) in [0.9, 0.5, 0.1].iter().enumerate() {
        answers.insert(
            format!("d{k}_usable_evidence"),
            json!({"type":"noul","noul":usable}),
        );
        for name in ["relevant", "contradicts", "injection"] {
            answers.insert(format!("d{k}_{name}"), json!({"type":"noul","noul":0.2}));
        }
    }
    let body = json!({"model":"jev-1.13.0","answers":answers,"usage":{"input_tokens":300,"output_tokens":20}});
    let (url, server) = scripted_server(vec![(0, "200 OK", body.to_string())]).await;
    let mut client = hedging_client(dir.path(), url, 1.0);
    client.hedge_after = None;
    // This test packs several chunks per call.
    client.batch = 4;
    let documents: Vec<Document> = ["one", "two", "three"]
        .iter()
        .map(|id| Document {
            id: (*id).into(),
            source_id: "s".into(),
            title: format!("Title {id}"),
            url: String::new(),
            text: format!("Text {id}"),
            provenance: Value::Null,
            raw_artifacts: vec![],
        })
        .collect();
    let scores = client
        .score_documents("How does it work?", &documents)
        .await;
    server.await.unwrap();
    let probabilities: Vec<f64> = scores
        .iter()
        .map(|s| s.as_ref().unwrap().probability)
        .collect();
    assert_eq!(probabilities, [0.9, 0.5, 0.1]);
    assert_eq!(scores[1].as_ref().unwrap().document_id, "two");
    assert_eq!(client.usage().requests, 1, "three chunks, one call");
}

#[tokio::test]
async fn claim_checks_map_answers_per_chunk_and_claim_and_split_escaped_text() {
    let dir = tempfile::tempdir().unwrap();
    // Two documents with one chunk each share a call at batch 4; each claim has its own call.
    let mut answers = serde_json::Map::new();
    for (k, supports, contradicts) in [(0, 0.9, 0.7), (1, 0.3, 0.0)] {
        answers.insert(
            format!("d{k}_c0_supports"),
            json!({"type":"noul","noul":supports}),
        );
        answers.insert(
            format!("d{k}_c0_contradicts"),
            json!({"type":"noul","noul":contradicts}),
        );
        answers.insert(
            format!("d{k}_c0_qualifies"),
            json!({"type":"noul","noul":0.1}),
        );
    }
    let body = json!({"model":"jev-1.13.0","answers":answers,"usage":{"input_tokens":300,"output_tokens":20}});
    let (url, server) = scripted_server(vec![
        (0, "200 OK", body.to_string()),
        (0, "200 OK", body.to_string()),
    ])
    .await;
    let mut client = hedging_client(dir.path(), url, 1.0);
    client.hedge_after = None;
    // This test packs several chunks per call.
    client.batch = 4;
    let documents = vec![
        text_document("one", "First text".into()),
        text_document("two", "Second text".into()),
    ];
    let claims = vec!["Claim A".to_owned(), "Claim B".to_owned()];
    let judged = client
        .judge_claims("A question?", &claims, &documents)
        .await;
    server.await.unwrap();
    let one = judged[0].as_ref().unwrap();
    assert_eq!(one.len(), 2);
    assert_eq!((one[0].supports, one[1].contradicts), (0.9, 0.7));
    let two = judged[1].as_ref().unwrap();
    assert_eq!(two[1].supports, 0.3);
    assert_eq!(client.usage().requests, 2, "one call per claim");
    // A chunk of control characters serializes six times larger; it is split to fit.
    let escaped = "\u{1}".repeat(DOCUMENT_CHUNK_BYTES);
    let pieces = fit_serialized("t", &escaped, (0, escaped.len()), CLAIM_CHUNK_STATE_BYTES);
    assert!(pieces.len() > 1);
    assert_eq!(
        pieces.iter().map(|(s, e)| e - s).sum::<usize>(),
        escaped.len()
    );
    assert!(pieces
        .iter()
        .all(
            |&(s, e)| json!({"title":"t","text":&escaped[s..e]}).to_string().len()
                <= CLAIM_CHUNK_STATE_BYTES
        ));
}

#[tokio::test]
async fn a_failed_call_for_one_claim_fails_the_documents_it_covered() {
    let dir = tempfile::tempdir().unwrap();
    let mut answers = serde_json::Map::new();
    for k in 0..2 {
        for name in ["supports", "contradicts", "qualifies"] {
            answers.insert(format!("d{k}_c0_{name}"), json!({"type":"noul","noul":0.2}));
        }
    }
    let body = json!({"model":"jev-1.13.0","answers":answers,"usage":{"input_tokens":300,"output_tokens":20}});
    // Four claims, two documents in one call each at batch 4: four calls, one of which fails.
    let (url, server) = scripted_server(vec![
        (0, "200 OK", body.to_string()),
        (0, "200 OK", body.to_string()),
        (0, "200 OK", body.to_string()),
        (0, "500 Internal Server Error", "{}".into()),
    ])
    .await;
    let mut client = hedging_client(dir.path(), url, 1.0);
    client.hedge_after = None;
    client.batch = 4;
    let documents = vec![
        text_document("one", "First text".into()),
        text_document("two", "Second text".into()),
    ];
    let claims: Vec<String> = (0..4).map(|j| format!("Claim {j}")).collect();
    let judged = client
        .judge_claims("A question?", &claims, &documents)
        .await;
    server.await.unwrap();
    assert_eq!(client.usage().requests, 4);
    assert!(judged
        .iter()
        .all(|j| failure_cause(j.as_ref().unwrap_err()) == Some("http_500")));
}

fn text_document(id: &str, text: String) -> Document {
    Document {
        id: id.into(),
        source_id: "s".into(),
        title: "Title".into(),
        url: String::new(),
        text,
        provenance: Value::Null,
        raw_artifacts: vec![],
    }
}

#[tokio::test]
async fn escaping_heavy_chunks_split_into_calls_that_fit_the_state_limit() {
    let dir = tempfile::tempdir().unwrap();
    // Each chunk is 12,000 raw bytes but about 18,000 once JSON-escaped.
    let documents: Vec<Document> = (0..3)
        .map(|i| text_document(&format!("d{i}"), "a\"".repeat(6_000)))
        .collect();
    let pair = {
        let mut answers = serde_json::Map::new();
        for k in 0..2 {
            for name in EVIDENCE_SIGNALS {
                answers.insert(format!("d{k}_{name}"), json!({"type":"noul","noul":0.6}));
            }
        }
        json!({"model":"jev-1.13.0","answers":answers,"usage":{"input_tokens":100,"output_tokens":1}})
    };
    let single = {
        let answers: serde_json::Map<String, Value> = EVIDENCE_SIGNALS
            .iter()
            .map(|n| ((*n).to_owned(), json!({"type":"noul","noul":0.6})))
            .collect();
        json!({"model":"jev-1.13.0","answers":answers,"usage":{"input_tokens":100,"output_tokens":1}})
    };
    let (url, server) = scripted_server(vec![
        (0, "200 OK", pair.to_string()),
        (0, "200 OK", single.to_string()),
    ])
    .await;
    let mut client = hedging_client(dir.path(), url, 1.0);
    client.hedge_after = None;
    // This test packs several chunks per call.
    client.batch = 4;
    // One permit keeps the calls in order, so each reply meets its own request.
    client.http = HttpRecorder::loopback_for_test(
        dir.path(),
        &RunConfig {
            fixture: false,
            timeout_secs: 5,
            concurrency: 1,
            ..RunConfig::default()
        },
    )
    .unwrap();
    let scores = client
        .score_documents("How does it work?", &documents)
        .await;
    server.await.unwrap();
    assert!(scores.iter().all(|s| s.is_ok()), "{scores:?}");
    assert_eq!(client.usage().requests, 2);
}

#[tokio::test]
async fn a_failed_shared_call_fails_every_document_in_it() {
    let dir = tempfile::tempdir().unwrap();
    let documents: Vec<Document> = (0..3)
        .map(|i| text_document(&format!("d{i}"), format!("Text {i}")))
        .collect();
    let (url, server) = scripted_server(vec![(0, "503 Service Unavailable", "{}".into())]).await;
    let mut client = hedging_client(dir.path(), url, 1.0);
    client.hedge_after = None;
    // This test packs several chunks per call.
    client.batch = 4;
    let scores = client
        .score_documents("How does it work?", &documents)
        .await;
    server.await.unwrap();
    // Every document keeps the call's cause class.
    assert!(scores
        .iter()
        .all(|s| failure_cause(s.as_ref().unwrap_err()) == Some("http_503")));
    assert_eq!(client.usage().requests, 1);
}

#[test]
fn provider_rate_overrides_are_strict() {
    let parsed = rpm_overrides(Some("typesafe=3000, cloudflare=600".into())).unwrap();
    assert_eq!(parsed["typesafe"], 3000.0);
    assert_eq!(parsed["cloudflare"], 600.0);
    assert!(rpm_overrides(None).unwrap().is_empty());
    for bad in [
        "typesafe",
        "typesafe=fast",
        "typesafe=0",
        "example=10",
        "typesafe=-5",
    ] {
        assert!(rpm_overrides(Some(bad.into())).is_err(), "{bad}");
    }
}

#[test]
fn a_provider_setting_that_cannot_be_a_header_fails_at_startup_by_name() {
    let env = |pairs: Vec<(&'static str, &'static str)>| {
        move |key: &str| {
            pairs
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.to_string())
        }
    };
    let account = ("CLOUDFLARE_ACCOUNT_ID", "0123456789abcdef0123456789abcdef");
    let bad = "fernlet\nsecret";
    for (setting, pairs) in [
        ("TYPESAFE_AI_API_KEY", vec![("TYPESAFE_AI_API_KEY", bad)]),
        ("OPENROUTER_API_KEY", vec![("OPENROUTER_API_KEY", bad)]),
        (
            "CLOUDFLARE_API_TOKEN",
            vec![account, ("CLOUDFLARE_API_TOKEN", bad)],
        ),
        (
            "JEV_GATEWAY_ID",
            vec![
                account,
                ("CLOUDFLARE_API_TOKEN", "t"),
                ("JEV_GATEWAY_ID", bad),
            ],
        ),
    ] {
        let text = providers_from(env(pairs), |_: &str| -> Result<String> {
            panic!("static token needs no profile")
        })
        .err()
        .map(|e| format!("{e:#}"))
        .unwrap_or_else(|| panic!("{setting} was accepted"));
        assert!(text.starts_with(setting), "{text}");
        assert!(!text.contains("fernlet"), "{text}");
    }
    // A token from a profile is checked the same way.
    let profile = env(vec![account, ("JEV_CLOUDFLARE_AUTH_PROFILE", "fernlet")]);
    let text = providers_from(profile, |_: &str| Ok(bad.to_owned()))
        .err()
        .map(|e| format!("{e:#}"))
        .unwrap();
    assert!(text.contains("JEV_CLOUDFLARE_AUTH_PROFILE"), "{text}");
    assert!(!text.contains("secret"), "{text}");
}

#[test]
fn the_provider_chain_follows_jev_providers_or_the_configured_default() {
    let env = |pairs: &'static [(&'static str, &'static str)]| {
        move |key: &str| {
            pairs
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.to_string())
        }
    };
    let no_profile = |_: &str| -> Result<String> { panic!("static token needs no profile") };
    let names = |chain: Vec<Backend>| chain.iter().map(|b| b.name()).collect::<Vec<_>>();
    const ALL: &[(&str, &str)] = &[
        ("CLOUDFLARE_ACCOUNT_ID", "0123456789abcdef0123456789abcdef"),
        ("CLOUDFLARE_API_TOKEN", "t"),
        ("TYPESAFE_AI_API_KEY", "k"),
        ("OPENROUTER_API_KEY", "o"),
    ];
    assert_eq!(
        names(providers_from(env(ALL), no_profile).unwrap()),
        ["cloudflare", "typesafe", "openrouter"]
    );
    assert_eq!(
        names(providers_from(env(&[("OPENROUTER_API_KEY", "o")]), no_profile).unwrap()),
        ["openrouter"]
    );
    const ORDERED: &[(&str, &str)] = &[
        ("TYPESAFE_AI_API_KEY", "k"),
        ("OPENROUTER_API_KEY", "o"),
        ("JEV_PROVIDERS", "openrouter, TypeSafe"),
    ];
    assert_eq!(
        names(providers_from(env(ORDERED), no_profile).unwrap()),
        ["openrouter", "typesafe"]
    );
    for bad in [
        &[("JEV_PROVIDERS", "typesafe")][..],
        &[
            ("TYPESAFE_AI_API_KEY", "k"),
            ("JEV_PROVIDERS", "typesafe,typesafe"),
        ][..],
        &[("TYPESAFE_AI_API_KEY", "k"), ("JEV_PROVIDERS", "vertex")][..],
        &[][..],
    ] {
        let bad: &'static [(&'static str, &'static str)] =
            Box::leak(bad.to_vec().into_boxed_slice());
        assert!(providers_from(env(bad), no_profile).is_err());
    }
    let (url, headers, body) = request_parts(
        &Backend::OpenRouter { key: "o".into() },
        &json!({"x":1}),
        &Map::from_iter([("q".into(), json!({}))]),
    )
    .unwrap();
    assert_eq!(url, "https://openrouter.ai/api/v1/systemone");
    assert!(headers.contains(&("authorization".into(), "Bearer o".into())));
    assert_eq!(
        (body["model"].as_str(), &body["state"]),
        (Some("~typesafe/jev-latest"), &json!({"x":1}))
    );
}

#[tokio::test]
async fn a_rate_limited_provider_hands_the_call_to_the_next_one_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let (busy, busy_server) =
        scripted_server(vec![(0, "429 Too Many Requests", "{}".into())]).await;
    let (spare, spare_server) = scripted_server(vec![
        (0, "200 OK", response().to_string()),
        (0, "200 OK", response().to_string()),
    ])
    .await;
    let mut client = hedging_client(dir.path(), busy, 1.0);
    client.hedge_after = None;
    // A real cooldown: the call must not wait for it.
    client.fallbacks = vec![Backend::Loopback {
        url: spare,
        token: "offline-placeholder".into(),
    }];
    client.providers = Mutex::new(vec![ProviderState::default(); 2]);
    let questions = || {
        Map::from_iter([
            ("a".into(), json!({"type":"noul"})),
            ("b".into(), json!({"type":"noul"})),
        ])
    };
    let started = std::time::Instant::now();
    for n in 0..2 {
        client
            .evaluate(json!({ "n": n }), questions(), json!({}))
            .await
            .unwrap();
    }
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "failover must not wait"
    );
    busy_server.await.unwrap();
    spare_server.await.unwrap();
    let usage = client.usage();
    // The second call skips the cooling provider entirely.
    assert_eq!((usage.requests, usage.rate_limited_requests), (2, 1));
    assert!(client.spending_stop_reason().is_none());
}

fn two_provider_client(dir: &Path, first: String, second: String) -> JevClient {
    let mut client = hedging_client(dir, first, 1.0);
    client.hedge_after = None;
    client.fallbacks = vec![Backend::Loopback {
        url: second,
        token: "offline-placeholder".into(),
    }];
    client.providers = Mutex::new(vec![ProviderState::default(); 2]);
    client
}

async fn ask(client: &JevClient) -> Result<(BTreeMap<String, f64>, String)> {
    client
        .evaluate(
            json!({"n":1}),
            Map::from_iter([
                ("a".into(), json!({"type":"noul"})),
                ("b".into(), json!({"type":"noul"})),
            ]),
            json!({}),
        )
        .await
}

/// A proxy that refuses every tunnel, as a restricted network does, and counts connections.
async fn denying_proxy() -> (String, Arc<std::sync::atomic::AtomicUsize>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = count.clone();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::spawn(async move {
                let mut input = [0; 4096];
                let _ = socket.read(&mut input).await;
                let _ = socket
                    .write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
                    .await;
            });
        }
    });
    (url, count)
}

/// A client for providers at `urls` whose every request meets `proxy`.
fn proxied_client(dir: &Path, proxy: &str, urls: &[&str]) -> JevClient {
    let mut client = JevClient::loopback_for_test(
        &RunConfig {
            output_dir: dir.to_path_buf(),
            budget_usd: 1.0,
            timeout_secs: 5,
            ..RunConfig::default()
        },
        urls[0].to_owned(),
    )
    .unwrap();
    client.http = client.http.through_proxy_for_test(proxy).unwrap();
    client.fallbacks = urls[1..]
        .iter()
        .map(|url| Backend::Loopback {
            url: (*url).to_owned(),
            token: "offline-placeholder".into(),
        })
        .collect();
    client.providers = Mutex::new(vec![ProviderState::default(); urls.len()]);
    client
}

/// A Jev server that reads each request and closes the connection without an answer, and
/// counts connections.
async fn hanging_up_server() -> (String, Arc<std::sync::atomic::AtomicUsize>) {
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/evaluate", listener.local_addr().unwrap());
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = count.clone();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::spawn(async move {
                let mut input = [0; 16384];
                let _ = socket.read(&mut input).await;
            });
        }
    });
    (url, count)
}

/// A loopback URL where nothing listens: the port was just released.
fn refusing_url() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    format!("http://{}/evaluate", listener.local_addr().unwrap())
}

/// The saved Jev traces of attempts that ended in `state`.
fn traces_in_state(dir: &Path, state: &str) -> Vec<Value> {
    std::fs::read_dir(dir.join("jev"))
        .unwrap()
        .map(|entry| {
            serde_json::from_slice::<Value>(&std::fs::read(entry.unwrap().path()).unwrap()).unwrap()
        })
        .filter(|trace| trace["state"] == state)
        .collect()
}

/// The probe rounds of each saved network check.
fn network_check_rounds(dir: &Path) -> Vec<u64> {
    std::fs::read_dir(dir.join("jev"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("network-check-")
        })
        .map(|path| {
            let record: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            record["rounds"].as_u64().unwrap()
        })
        .collect()
}

/// Saved network check records.
fn network_checks(dir: &Path) -> usize {
    std::fs::read_dir(dir.join("jev"))
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("network-check-")
        })
        .count()
}

#[tokio::test]
async fn a_connection_that_fails_before_sending_releases_its_reservation_and_never_stops() {
    let refused_dir = tempfile::tempdir().unwrap();
    let refused = JevClient::loopback_for_test(
        &RunConfig {
            output_dir: refused_dir.path().to_path_buf(),
            budget_usd: 1.0,
            timeout_secs: 5,
            ..RunConfig::default()
        },
        refusing_url(),
    )
    .unwrap();
    let denied_dir = tempfile::tempdir().unwrap();
    let (proxy, connections) = denying_proxy().await;
    let denied = proxied_client(
        denied_dir.path(),
        &proxy,
        &["https://fernlet.test/evaluate"],
    );
    let calls = UNRESOLVED_STOP_AFTER + 1;
    for (mut client, dir, cause) in [
        (refused, refused_dir.path(), "connect_refused"),
        (denied, denied_dir.path(), "connect_denied"),
    ] {
        client.rate_limit_wait = Some(Duration::from_millis(20));
        // The first call fails to connect. The network check that follows fails too, so the
        // later calls fail at once with its cause, and none of them counts toward the stop.
        let error = evaluate_one(&client).await.unwrap_err();
        assert_eq!(failure_cause(&error), Some(cause));
        let text = format!("{error:#}");
        assert!(
            text.starts_with(&format!("Jev could not connect ({cause}); the request was not sent, and its reservation was released.")),
            "{text}"
        );
        assert!(!text.contains("offline-placeholder") && !text.contains("fernlet.test"));
        for _ in 1..calls {
            let error = evaluate_one(&client).await.unwrap_err();
            assert_eq!(failure_cause(&error), Some(cause));
            assert!(!not_assessed_after_stop(&error));
            assert_eq!(
                error.to_string(),
                format!("The Jev network check after a connect failure found no reachable provider (loopback: {cause}); nothing was reserved")
            );
        }
        assert!(client.spending_stop_reason().is_none());
        let usage = client.usage();
        assert_eq!((usage.requests, usage.cost_usd), (0, 0.0));
        let ledger = client.ledger.lock().unwrap();
        assert_eq!(ledger.accounted_nanos, 0);
        assert_eq!((ledger.in_flight, ledger.consecutive_unresolved), (0, 0));
        assert!(!ledger.unresolved_usage && ledger.last_unresolved_cause.is_none());
        drop(ledger);
        let traces = traces_in_state(dir, "connect_failed");
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0]["transport_cause"], cause);
        assert!(traces[0]["accounting"]
            .as_str()
            .unwrap()
            .starts_with("Reservation released"));
        let text = traces[0].to_string();
        assert!(!text.contains("offline-placeholder") && !text.contains("fernlet.test"));
        assert!(traces_in_state(dir, "transport_error_or_incomplete_body").is_empty());
        // One check, with a round before and after each of the 3 pauses.
        assert_eq!(network_check_rounds(dir), [4]);
    }
    // One tunnel for the paid attempt and one for each round of the network check.
    assert_eq!(connections.load(std::sync::atomic::Ordering::SeqCst), 5);
}

/// Name resolution for offline tests: the first `failures` lookups fail, as in a network
/// outage, and later ones answer with the loopback address. The first lookup takes
/// `first_delay`, and every later one `later_delay`. It counts lookups.
struct FlakyDns {
    failures: std::sync::atomic::AtomicUsize,
    lookups: std::sync::atomic::AtomicUsize,
    first_delay: Duration,
    later_delay: Duration,
}

impl FlakyDns {
    fn failing(failures: usize) -> Arc<Self> {
        Self::with_delays(failures, Duration::ZERO, Duration::ZERO)
    }
    fn slow_after_first(failures: usize, later_delay: Duration) -> Arc<Self> {
        Self::with_delays(failures, Duration::ZERO, later_delay)
    }
    fn slow(failures: usize, delay: Duration) -> Arc<Self> {
        Self::with_delays(failures, delay, delay)
    }
    fn with_delays(failures: usize, first_delay: Duration, later_delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            failures: failures.into(),
            lookups: 0.into(),
            first_delay,
            later_delay,
        })
    }
}

impl reqwest::dns::Resolve for FlakyDns {
    fn resolve(&self, _: reqwest::dns::Name) -> reqwest::dns::Resolving {
        use std::sync::atomic::Ordering::SeqCst;
        let delay = if self.lookups.fetch_add(1, SeqCst) == 0 {
            self.first_delay
        } else {
            self.later_delay
        };
        let failed = self
            .failures
            .fetch_update(SeqCst, SeqCst, |n| n.checked_sub(1))
            .is_ok();
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            if failed {
                return Err("no answer".into());
            }
            let loopback = std::net::SocketAddr::from(([127, 0, 0, 1], 0));
            Ok(Box::new(std::iter::once(loopback)) as reqwest::dns::Addrs)
        })
    }
}

/// A client for providers at `urls` whose names resolve through `dns`.
fn resolving_client(dir: &Path, dns: Arc<FlakyDns>, urls: &[String]) -> JevClient {
    let mut client = JevClient::loopback_for_test(
        &RunConfig {
            output_dir: dir.to_path_buf(),
            budget_usd: 1.0,
            timeout_secs: 5,
            ..RunConfig::default()
        },
        urls[0].clone(),
    )
    .unwrap();
    client.http = client.http.with_resolver_for_test(dns).unwrap();
    client.fallbacks = urls[1..]
        .iter()
        .map(|url| Backend::Loopback {
            url: url.clone(),
            token: "offline-placeholder".into(),
        })
        .collect();
    client.providers = Mutex::new(vec![ProviderState::default(); urls.len()]);
    client
}

/// A Jev server that answers every connection with a valid response, and counts connections.
async fn answering_server() -> (u16, Arc<std::sync::atomic::AtomicUsize>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = count.clone();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::spawn(async move {
                let mut input = [0; 16384];
                let _ = socket.read(&mut input).await;
                let body = response().to_string();
                let reply = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(reply.as_bytes()).await;
                let _ = socket.shutdown().await;
            });
        }
    });
    (port, count)
}

#[tokio::test]
async fn a_sustained_connect_outage_fails_later_calls_at_once_and_for_free() {
    use futures::StreamExt;
    let dir = tempfile::tempdir().unwrap();
    let dns = FlakyDns::failing(usize::MAX);
    let mut client = resolving_client(
        dir.path(),
        dns.clone(),
        &[
            "http://fernlet.test:9/evaluate".into(),
            "http://birch.fernlet.test:9/evaluate".into(),
        ],
    );
    // A cooldown far longer than the test may take: no call may wait it out.
    client.rate_limit_wait = Some(Duration::from_secs(30));
    let concurrency = 8;
    let started = std::time::Instant::now();
    let errors: Vec<anyhow::Error> = futures::stream::iter(0..concurrency * 5)
        .map(|_| evaluate_one(&client))
        .buffer_unordered(concurrency)
        .map(|result| result.unwrap_err())
        .collect()
        .await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    let offline = "The Jev network check after a connect failure found no reachable provider (loopback: dns, loopback: dns); nothing was reserved";
    for error in &errors {
        assert_eq!(failure_cause(error), Some("dns"));
        assert!(!not_assessed_after_stop(error));
    }
    // Only calls of the first wave can end with their own connect failure.
    let own = errors.iter().filter(|e| e.to_string() != offline).count();
    assert!((1..=concurrency).contains(&own), "{own}");
    // One shared network check, and nothing spent or kept.
    assert_eq!(network_checks(dir.path()), 1);
    assert!(client.spending_stop_reason().is_none());
    let usage = client.usage();
    assert_eq!((usage.requests, usage.cost_usd), (0, 0.0));
    let ledger = client.ledger.lock().unwrap();
    assert_eq!(ledger.accounted_nanos, 0);
    assert_eq!((ledger.in_flight, ledger.consecutive_unresolved), (0, 0));
    drop(ledger);
    assert_eq!(network_check_rounds(dir.path()), [4]);
    // Lookups: at most one per provider for each call of the first wave, and one per provider
    // for each of the check's 4 rounds.
    let lookups = dns.lookups.load(std::sync::atomic::Ordering::SeqCst);
    assert!(lookups <= concurrency * 2 + 2 * 4, "{lookups}");
}

#[tokio::test]
async fn a_call_waiting_for_a_cooling_provider_ends_when_the_network_check_fails() {
    let dir = tempfile::tempdir().unwrap();
    // The first call fails to connect at once. Each lookup of the network check then takes
    // 1 s and fails, so the second call starts while the provider cools and the check runs.
    // After the first 1 s round, a pause would pass the 500 ms window, so no round follows.
    let dns = FlakyDns::slow_after_first(usize::MAX, Duration::from_secs(1));
    let mut client = resolving_client(dir.path(), dns, &["http://fernlet.test:9/evaluate".into()]);
    client.rate_limit_wait = Some(Duration::from_secs(30));
    client.recheck_window = Duration::from_millis(500);
    let first = evaluate_one(&client);
    let second = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let started = std::time::Instant::now();
        (evaluate_one(&client).await, started.elapsed())
    };
    let (first, (second, waited)) = tokio::join!(first, second);
    assert!(first
        .unwrap_err()
        .to_string()
        .starts_with("Jev could not connect (dns);"));
    // The second call waited only until the check failed (about 0.7 s), not for the cooldown
    // or for one capped wait of it (5 s).
    assert!(waited < Duration::from_secs(3), "{waited:?}");
    assert_eq!(
        second.unwrap_err().to_string(),
        "The Jev network check after a connect failure found no reachable provider (loopback: dns); nothing was reserved"
    );
    assert!(client.usage().provider_wait_ms > 0);
    assert_eq!(client.usage().requests, 0);
    assert_eq!(network_check_rounds(dir.path()), [1]);
}

#[tokio::test]
async fn a_wait_ended_by_the_offline_state_counts_only_the_time_waited() {
    let dir = tempfile::tempdir().unwrap();
    // The first call fails to connect at once and starts a network check of one 1 s round,
    // which fails. The second call starts at 300 ms, while the provider cools for 3 s, so it
    // plans to wait the rest of the cooldown (about 2.7 s). The failed check ends that wait
    // after about 0.7 s.
    let dns = FlakyDns::slow_after_first(usize::MAX, Duration::from_secs(1));
    let mut client = resolving_client(dir.path(), dns, &["http://fernlet.test:9/evaluate".into()]);
    client.rate_limit_wait = Some(Duration::from_secs(3));
    client.recheck_window = Duration::from_millis(500);
    let first = evaluate_one(&client);
    let second = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let started = std::time::Instant::now();
        (evaluate_one(&client).await, started.elapsed())
    };
    let (first, (second, elapsed)) = tokio::join!(first, second);
    assert!(first.is_err() && second.is_err());
    assert!(client.offline.borrow().is_some());
    let waited = client.usage().provider_wait_ms;
    assert!(waited > 0);
    assert!(
        waited <= elapsed.as_millis() as u64,
        "{waited} > {elapsed:?}"
    );
    assert!(waited < 2_000, "{waited} ms counts the planned wait");
}

#[tokio::test]
async fn calls_succeed_again_when_the_network_returns_within_the_check_window() {
    let dir = tempfile::tempdir().unwrap();
    let (port, connections) = answering_server().await;
    // The paid attempt's lookup and the check's first 2 rounds fail. The third round, after
    // the first 2 pauses, finds the network back.
    let dns = FlakyDns::failing(3);
    let mut client = resolving_client(
        dir.path(),
        dns,
        &[format!("http://fernlet.test:{port}/evaluate")],
    );
    client.rate_limit_wait = Some(Duration::from_millis(50));
    let error = ask(&client).await.unwrap_err();
    assert_eq!(failure_cause(&error), Some("dns"));
    assert!(error
        .to_string()
        .starts_with("Jev could not connect (dns);"));
    assert_eq!(network_check_rounds(dir.path()), [3]);
    assert!(client.offline.borrow().is_none());
    // The check passed, so later calls wait out the short cooldown and succeed.
    for _ in 0..3 {
        assert_eq!(ask(&client).await.unwrap().0.len(), 2);
    }
    // The passing round and the three answered calls.
    assert_eq!(connections.load(std::sync::atomic::Ordering::SeqCst), 4);
    assert!(client.spending_stop_reason().is_none());
    assert_eq!(client.usage().requests, 3);
    let ledger = client.ledger.lock().unwrap();
    assert_eq!(
        ledger.accounted_nanos,
        3 * client.backend.cost_nanos(100).unwrap()
    );
    assert_eq!((ledger.in_flight, ledger.consecutive_unresolved), (0, 0));
}

#[tokio::test]
async fn a_wave_of_connect_failures_shares_one_network_check() {
    use futures::StreamExt;
    let dir = tempfile::tempdir().unwrap();
    let (port, _) = answering_server().await;
    // Each lookup takes 100 ms, so the 8 calls of the first wave all look up before any
    // fails. Those 8 lookups and the check's first round fail; its second round passes.
    let concurrency = 8;
    let dns = FlakyDns::slow(concurrency + 1, Duration::from_millis(100));
    let mut client = resolving_client(
        dir.path(),
        dns,
        &[format!("http://fernlet.test:{port}/evaluate")],
    );
    client.rate_limit_wait = Some(Duration::from_millis(50));
    let results: Vec<_> = futures::stream::iter(0..concurrency * 2)
        .map(|_| ask(&client))
        .buffer_unordered(concurrency)
        .collect()
        .await;
    let failed: Vec<_> = results.iter().filter_map(|r| r.as_ref().err()).collect();
    assert_eq!(failed.len(), concurrency);
    for error in failed {
        assert!(error
            .to_string()
            .starts_with("Jev could not connect (dns);"));
    }
    // Every call of the wave waited for the one check and used its result.
    assert_eq!(network_check_rounds(dir.path()), [2]);
    assert!(client.offline.borrow().is_none());
    assert_eq!(client.usage().requests, concurrency as u64);
    assert!(client.spending_stop_reason().is_none());
}

#[tokio::test]
async fn a_hedge_that_cannot_connect_releases_its_reservation() {
    let dir = tempfile::tempdir().unwrap();
    // The first provider answers after 400 ms. The hedge starts at 100 ms and goes to the
    // second provider, which refuses the connection.
    let (url, server) = scripted_server(vec![(400, "200 OK", response().to_string())]).await;
    let mut client = two_provider_client(dir.path(), url, refusing_url());
    client.hedge_after = Some(Duration::from_millis(100));
    let (answers, _) = ask(&client).await.unwrap();
    assert_eq!(answers.len(), 2);
    server.await.unwrap();
    let usage = client.usage();
    assert_eq!((usage.requests, usage.hedged_requests), (1, 0));
    let ledger = client.ledger.lock().unwrap();
    assert_eq!(
        ledger.accounted_nanos,
        client.backend.cost_nanos(100).unwrap(),
        "only the answered request costs"
    );
    assert_eq!((ledger.in_flight, ledger.consecutive_unresolved), (0, 0));
    drop(ledger);
    let failed = traces_in_state(dir.path(), "connect_failed");
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["attempt"], 1);
    assert_eq!(failed[0]["transport_cause"], "connect_refused");
    assert!(client.spending_stop_reason().is_none());
}

#[tokio::test]
async fn a_request_refused_before_sending_releases_its_reservation_and_never_stops() {
    let (url, connections) = hanging_up_server().await;
    // A recorder in fixture mode refuses every request; a recorder without loopback
    // permission refuses the plain-HTTP URL. Both refusals come before the send point.
    for (fixture, refusal) in [
        (true, "Network requests are forbidden in fixture mode"),
        (false, "Request URL must use HTTPS"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let config = RunConfig {
            output_dir: dir.path().to_path_buf(),
            budget_usd: 1.0,
            ..RunConfig::default()
        };
        let mut client = JevClient::loopback_for_test(&config, url.clone()).unwrap();
        client.http = HttpRecorder::new(dir.path(), &RunConfig { fixture, ..config }).unwrap();
        let calls = UNRESOLVED_STOP_AFTER + 1;
        for _ in 0..calls {
            let error = evaluate_one(&client).await.unwrap_err();
            assert_eq!(failure_cause(&error), Some("other"));
            assert!(!not_assessed_after_stop(&error));
            let text = error.to_string();
            assert!(
                text.starts_with(&format!("Jev request was refused before it was sent ({refusal}); the reservation was released.")),
                "{text}"
            );
        }
        assert!(client.spending_stop_reason().is_none());
        let usage = client.usage();
        assert_eq!((usage.requests, usage.cost_usd), (0, 0.0));
        let ledger = client.ledger.lock().unwrap();
        assert_eq!(ledger.accounted_nanos, 0);
        assert_eq!((ledger.in_flight, ledger.consecutive_unresolved), (0, 0));
        assert!(!ledger.unresolved_usage);
        drop(ledger);
        assert_eq!(
            traces_in_state(dir.path(), "refused_before_send").len() as u32,
            calls
        );
    }
    assert_eq!(connections.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_refused_connection_moves_the_call_to_the_next_provider_for_free() {
    let dir = tempfile::tempdir().unwrap();
    let (url, server) = scripted_server(vec![(0, "200 OK", response().to_string())]).await;
    let client = two_provider_client(dir.path(), refusing_url(), url);
    let (answers, _) = ask(&client).await.unwrap();
    assert_eq!(answers.len(), 2);
    server.await.unwrap();
    assert!(client.spending_stop_reason().is_none());
    assert_eq!(client.usage().requests, 1);
    let ledger = client.ledger.lock().unwrap();
    assert_eq!(
        ledger.accounted_nanos,
        client.backend.cost_nanos(100).unwrap(),
        "only the answered request costs"
    );
    assert_eq!((ledger.in_flight, ledger.consecutive_unresolved), (0, 0));
    drop(ledger);
    let failed = traces_in_state(dir.path(), "connect_failed");
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["transport_cause"], "connect_refused");
    assert_eq!(traces_in_state(dir.path(), "complete").len(), 1);
}

#[tokio::test]
async fn a_failure_after_the_request_was_sent_keeps_its_reservation_and_stops() {
    let dir = tempfile::tempdir().unwrap();
    let (first, _) = hanging_up_server().await;
    let (second, second_connections) = hanging_up_server().await;
    let client = two_provider_client(dir.path(), first, second);
    let reservation = client.backend.reservation().unwrap();
    for _ in 0..UNRESOLVED_STOP_AFTER {
        let error = evaluate_one(&client).await.unwrap_err();
        assert_eq!(failure_cause(&error), Some("connection_closed"));
        assert!(!not_assessed_after_stop(&error));
        let text = format!("{error:#}");
        assert!(
            text.starts_with("Jev transport failed (connection_closed) after the request may have been sent; the reservation remains charged."),
            "{text}"
        );
    }
    // Each sent attempt keeps its reservation. The next call reserves nothing, and its item
    // is not assessed.
    let refused = evaluate_one(&client).await.unwrap_err();
    assert!(not_assessed_after_stop(&refused));
    assert!(refused
        .to_string()
        .contains("(last cause: connection_closed)"));
    let stop = u64::from(UNRESOLVED_STOP_AFTER);
    assert_eq!(client.usage().requests, stop);
    assert_eq!(
        client.ledger.lock().unwrap().accounted_nanos,
        reservation * stop
    );
    // The provider may have run the request, so no other provider receives it.
    assert_eq!(
        second_connections.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    let traces = traces_in_state(dir.path(), "transport_error_or_incomplete_body");
    assert_eq!(traces.len() as u64, stop);
    for trace in &traces {
        assert_eq!(trace["transport_cause"], "connection_closed");
    }
    assert!(traces_in_state(dir.path(), "connect_failed").is_empty());
}

#[tokio::test]
async fn a_failed_network_check_reserves_nothing_and_names_the_cause() {
    let dir = tempfile::tempdir().unwrap();
    let (proxy, connections) = denying_proxy().await;
    let mut client = proxied_client(
        dir.path(),
        &proxy,
        &[
            "https://fernlet.test/evaluate",
            "https://birch.fernlet.test/evaluate",
        ],
    );
    client.network = tokio::sync::OnceCell::new();
    for _ in 0..2 {
        let error = evaluate_one(&client).await.unwrap_err();
        assert_eq!(failure_cause(&error), Some("connect_denied"));
        let text = error.to_string();
        assert!(text.contains("network check"), "{text}");
        assert!(text.contains("loopback: connect_denied, loopback: connect_denied"));
        assert!(text.ends_with("nothing was reserved"));
    }
    // One check per client, one request per provider, and no reservation.
    assert_eq!(connections.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(client.usage().requests, 0);
    assert_eq!(client.usage().cost_usd, 0.0);
    assert_eq!(client.ledger.lock().unwrap().accounted_nanos, 0);
    let checks: Vec<std::path::PathBuf> = std::fs::read_dir(dir.path().join("jev"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.to_string_lossy().contains("network-check-"))
        .collect();
    assert_eq!(checks.len(), 1);
    let record: Value = serde_json::from_slice(&std::fs::read(&checks[0]).unwrap()).unwrap();
    assert_eq!(
        record["providers"][1],
        json!({"provider":"loopback","reachable":false,"cause":"connect_denied"})
    );
}

#[tokio::test]
async fn a_provider_that_does_not_answer_the_network_check_is_not_used() {
    let dir = tempfile::tempdir().unwrap();
    let closed = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    // The second provider answers the check with any status, after the first one's refusal,
    // then the paid request.
    let (second, server) = scripted_server(vec![
        (200, "404 Not Found", String::new()),
        (0, "200 OK", response().to_string()),
    ])
    .await;
    let mut client = two_provider_client(dir.path(), format!("http://{closed}/evaluate"), second);
    client.network = tokio::sync::OnceCell::new();
    let (answers, _) = ask(&client).await.unwrap();
    server.await.unwrap();
    assert_eq!(answers.len(), 2);
    assert!(client.providers.lock().unwrap()[0].disabled);
    assert_eq!(
        client.skipped_providers(),
        BTreeMap::from([("loopback".into(), "connect_refused".into())])
    );
    assert_eq!(client.usage().requests, 1);
}

#[tokio::test]
async fn the_check_ends_at_the_first_reachable_provider_and_a_pending_one_stays_enabled() {
    let dir = tempfile::tempdir().unwrap();
    // The first provider connects and stays silent; the second answers at once.
    let (silent, _) = scripted_server(vec![(20_000, "200 OK", String::new())]).await;
    let (answers, _) = scripted_server(vec![(0, "404 Not Found", String::new())]).await;
    let mut client = two_provider_client(dir.path(), silent, answers);
    client.network = tokio::sync::OnceCell::new();
    let started = std::time::Instant::now();
    client.check_network().await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "the check waited for the silent provider"
    );
    assert!(client.providers.lock().unwrap().iter().all(|p| !p.disabled));
    assert!(client.skipped_providers().is_empty());
}

#[tokio::test]
async fn a_short_request_limit_does_not_shorten_the_network_check() {
    let dir = tempfile::tempdir().unwrap();
    // The origin connects and answers only after the check's wait.
    let (slow, _) = scripted_server(vec![(20_000, "404 Not Found", String::new())]).await;
    let mut client = JevClient::loopback_for_test(
        &RunConfig {
            output_dir: dir.path().to_path_buf(),
            budget_usd: 1.0,
            // Below the check's 6-second wait.
            timeout_secs: 2,
            ..RunConfig::default()
        },
        slow,
    )
    .unwrap();
    client.network = tokio::sync::OnceCell::new();
    client.check_network().await.unwrap();
    assert!(!client.providers.lock().unwrap()[0].disabled);
    assert!(client.skipped_providers().is_empty());
}

/// The whole request on `socket`: its head and its Content-Length body.
async fn read_request_text(socket: &mut tokio::net::TcpStream) -> String {
    use tokio::io::AsyncReadExt;
    let mut bytes = Vec::new();
    let mut part = [0; 8192];
    loop {
        let n = socket.read(&mut part).await.unwrap_or(0);
        if n == 0 {
            break;
        }
        bytes.extend_from_slice(&part[..n]);
        if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
            let length: usize = head
                .lines()
                .find_map(|line| line.strip_prefix("content-length:"))
                .map_or(0, |value| value.trim().parse().unwrap());
            if bytes.len() >= end + 4 + length {
                break;
            }
        }
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Two paid requests at once. The one that carries `marker` loses its connection, which stops
/// a client that stops after one unresolved attempt. Then the other one gets a 429, and its
/// retry is refused after the stop.
async fn stop_race_server(marker: &'static str) -> (String, tokio::task::JoinHandle<()>) {
    use tokio::io::AsyncWriteExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/evaluate", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut held = None;
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            if !read_request_text(&mut socket).await.contains(marker) {
                held = Some(socket);
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        let mut socket = held.unwrap();
        let _ = socket
            .write_all(
                b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .await;
        let _ = socket.shutdown().await;
    });
    (url, server)
}

fn stopping_client(dir: &Path, url: String) -> JevClient {
    let mut client = hedging_client(dir, url, 1.0);
    client.hedge_after = None;
    client.rate_limit_wait = Some(Duration::from_millis(20));
    client.ledger.lock().unwrap().unresolved_stop_after = 1;
    client
}

#[tokio::test]
async fn a_real_failure_outranks_a_later_stop_refusal_of_the_same_item() {
    // Scoring: chunk 0 is refused after the stop that chunk 1's failure caused.
    let dir = tempfile::tempdir().unwrap();
    let (url, server) = stop_race_server("beta").await;
    let client = stopping_client(dir.path(), url);
    let text = format!("{}{}", "alpha ".repeat(2_000), "beta ".repeat(200));
    let scores = client
        .score_documents("How do fernlets settle?", &[text_document("d", text)])
        .await;
    server.await.unwrap();
    let error = scores[0].as_ref().unwrap_err();
    assert!(!not_assessed_after_stop(error), "{error:#}");
    assert_eq!(failure_cause(error), Some("connection_closed"));
    assert_eq!(client.usage().requests, 1);

    // Claims: claim 0 is refused after the stop that claim 1's failure caused.
    let dir = tempfile::tempdir().unwrap();
    let (url, server) = stop_race_server("beta").await;
    let client = stopping_client(dir.path(), url);
    let claims = [
        "Fernlets settle in alpha.".to_owned(),
        "Fernlets settle in beta.".to_owned(),
    ];
    let judged = client
        .judge_claims(
            "How do fernlets settle?",
            &claims,
            &[text_document("d", "Fernlet text".into())],
        )
        .await;
    server.await.unwrap();
    let error = judged[0].as_ref().unwrap_err();
    assert!(!not_assessed_after_stop(error), "{error:#}");
    assert_eq!(failure_cause(error), Some("connection_closed"));
}

#[test]
fn provider_origins_need_no_credentials() {
    assert_eq!(
        provider_origin("cloudflare").unwrap(),
        "https://api.cloudflare.com/"
    );
    assert_eq!(
        provider_origin("typesafe").unwrap(),
        "https://api.typesafe.ai/"
    );
    assert_eq!(
        provider_origin("openrouter").unwrap(),
        "https://openrouter.ai/"
    );
    assert!(provider_origin("fernlet").is_err());
}

#[tokio::test]
async fn a_payment_refusal_cools_one_provider_and_is_not_a_rate_limit() {
    let dir = tempfile::tempdir().unwrap();
    let (first, a) = scripted_server(vec![(0, "402 Payment Required", "{}".into())]).await;
    let (second, b) = scripted_server(vec![(0, "200 OK", response().to_string())]).await;
    let client = two_provider_client(dir.path(), first, second);
    ask(&client).await.unwrap();
    a.await.unwrap();
    b.await.unwrap();
    let usage = client.usage();
    assert_eq!((usage.requests, usage.rate_limited_requests), (1, 0));
    assert!(!client.send_now(0));
    assert!(client.spending_stop_reason().is_none());
}

#[tokio::test]
async fn when_every_provider_is_cooling_the_call_waits_then_resumes() {
    let dir = tempfile::tempdir().unwrap();
    let (first, a) = scripted_server(vec![
        (0, "429 Too Many Requests", "{}".into()),
        (0, "200 OK", response().to_string()),
    ])
    .await;
    let (second, b) = scripted_server(vec![(0, "429 Too Many Requests", "{}".into())]).await;
    let mut client = two_provider_client(dir.path(), first, second);
    client.rate_limit_wait = Some(Duration::from_millis(50));
    ask(&client).await.unwrap();
    a.await.unwrap();
    b.await.unwrap();
    let usage = client.usage();
    assert_eq!((usage.requests, usage.rate_limited_requests), (1, 2));
}

#[tokio::test]
async fn a_hedge_goes_to_a_different_provider() {
    let dir = tempfile::tempdir().unwrap();
    // The first provider stalls; the hedge must reach the second one, which answers.
    let (first, a) = scripted_server(vec![(400, "200 OK", response().to_string())]).await;
    let (second, b) = scripted_server(vec![(0, "200 OK", response().to_string())]).await;
    let mut client = two_provider_client(dir.path(), first, second);
    client.hedge_after = Some(Duration::from_millis(20));
    let started = std::time::Instant::now();
    ask(&client).await.unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(350),
        "the hedge answered first"
    );
    b.await.unwrap();
    a.await.unwrap();
    assert_eq!(client.usage().hedged_requests, 1);
}

#[tokio::test]
async fn rejected_credentials_disable_one_provider_and_the_next_one_answers() {
    let dir = tempfile::tempdir().unwrap();
    let (bad, bad_server) = scripted_server(vec![(0, "401 Unauthorized", "{}".into())]).await;
    let (good, good_server) = scripted_server(vec![(0, "200 OK", response().to_string())]).await;
    let mut client = hedging_client(dir.path(), bad, 1.0);
    client.hedge_after = None;
    client.fallbacks = vec![Backend::Loopback {
        url: good,
        token: "offline-placeholder".into(),
    }];
    client.providers = Mutex::new(vec![ProviderState::default(); 2]);
    client
        .evaluate(
            json!({"n":1}),
            Map::from_iter([
                ("a".into(), json!({"type":"noul"})),
                ("b".into(), json!({"type":"noul"})),
            ]),
            json!({}),
        )
        .await
        .unwrap();
    bad_server.await.unwrap();
    good_server.await.unwrap();
    assert!(client.spending_stop_reason().is_none());
    assert!(client.providers.lock().unwrap()[0].disabled);
    assert_eq!(
        client.usage().rate_limited_requests,
        0,
        "a 401 is not a rate limit"
    );
    let ledger = client.ledger.lock().unwrap();
    assert_eq!(
        ledger.accounted_nanos,
        client.backend.cost_nanos(100).unwrap(),
        "the rejected request costs nothing"
    );
}

#[tokio::test]
async fn a_hedge_without_budget_room_leaves_the_first_attempt_to_finish() {
    let dir = tempfile::tempdir().unwrap();
    let (url, server) = scripted_server(vec![(400, "200 OK", response().to_string())]).await;
    let reservation = {
        let probe = client(dir.path(), 1.0);
        probe.backend.reservation().unwrap()
    };
    // Room for one reservation only.
    let budget = reservation as f64 * 1.5 / NANOS_PER_USD;
    let client = hedging_client(dir.path(), url, budget);
    let (answers, _) = client
        .evaluate(
            json!({"fixture":"hedge"}),
            Map::from_iter([
                ("a".into(), json!({"type":"noul"})),
                ("b".into(), json!({"type":"noul"})),
            ]),
            json!({}),
        )
        .await
        .unwrap();
    assert_eq!(answers.len(), 2);
    server.await.unwrap();
    let usage = client.usage();
    assert_eq!((usage.requests, usage.hedged_requests), (1, 0));
    assert!(client.spending_stop_reason().is_none());
    assert_eq!(client.ledger.lock().unwrap().in_flight, 0);
}

async fn evaluate_one(client: &JevClient) -> Result<(BTreeMap<String, f64>, String)> {
    client
        .evaluate(
            json!({"fixture":true}),
            Map::from_iter([("a".into(), json!({"type":"noul"}))]),
            json!({}),
        )
        .await
}

#[tokio::test]
async fn incomplete_and_unreceipted_schema_responses_stop_but_known_usage_stays_distinct() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    for (body, incomplete, known) in [
        ("{}".to_string(), false, false),
        ("{}".to_string(), true, false),
        (response().to_string(), false, true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/evaluate", listener.local_addr().unwrap());
        let client = JevClient::loopback_for_test(
            &RunConfig {
                output_dir: dir.path().to_path_buf(),
                budget_usd: 1.0,
                timeout_secs: 2,
                ..RunConfig::default()
            },
            url,
        )
        .unwrap();
        client.ledger.lock().unwrap().unresolved_stop_after = 1;
        let reservation = client.backend.reservation().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut input = [0; 8192];
            assert!(socket.read(&mut input).await.unwrap() > 0);
            let length = body.len() + if incomplete { 100 } else { 0 };
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n{body}").as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
            listener
        });
        // response() contains an extra answer key, so even known usage fails schema validation.
        assert!(evaluate_one(&client).await.is_err());
        let listener = server.await.unwrap();
        assert_eq!(client.ledger.lock().unwrap().unresolved_usage, !known);
        assert_eq!(client.spending_stop_reason().is_none(), known);
        if !known {
            assert!(evaluate_one(&client).await.is_err());
            assert_eq!(client.usage().requests, 1);
            assert_eq!(client.ledger.lock().unwrap().accounted_nanos, reservation);
            assert!(
                tokio::time::timeout(Duration::from_millis(30), listener.accept())
                    .await
                    .is_err()
            );
        } else {
            assert_eq!(client.usage().input_tokens, 100);
            assert_eq!(
                client.ledger.lock().unwrap().accounted_nanos,
                client.backend.cost_nanos(100).unwrap()
            );
        }
        let traces: Vec<Value> = std::fs::read_dir(&client.audit_dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.to_string_lossy().contains("-attempt-"))
            .map(|p| serde_json::from_slice(&std::fs::read(p).unwrap()).unwrap())
            .collect();
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0]["usage_receipt_accounted"] == true, known);
    }
}

#[tokio::test]
async fn cancellation_and_initial_audit_failure_retain_reservations_and_stop() {
    use tokio::net::TcpListener;
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = RunConfig {
        output_dir: dir.path().to_path_buf(),
        budget_usd: 1.0,
        timeout_secs: 2,
        ..RunConfig::default()
    };
    let client = Arc::new(
        JevClient::loopback_for_test(
            &config,
            format!("http://{}/evaluate", listener.local_addr().unwrap()),
        )
        .unwrap(),
    );
    let task_client = client.clone();
    let task = tokio::spawn(async move { evaluate_one(&task_client).await });
    let (_socket, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
        .await
        .unwrap()
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(client
        .spending_stop_reason()
        .unwrap()
        .contains("unresolved"));
    assert!(evaluate_one(&client).await.is_err());
    assert_eq!(client.usage().requests, 1);
    assert_eq!(
        client.ledger.lock().unwrap().accounted_nanos,
        client.backend.reservation().unwrap()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );

    let other = tempfile::tempdir().unwrap();
    let mut broken = self::client(other.path(), 1.0);
    broken.backend = offline_cloudflare();
    broken.audit_dir = other.path().join("missing").join("jev");
    assert!(evaluate_one(&broken).await.is_err());
    assert!(broken
        .spending_stop_reason()
        .unwrap()
        .contains("unresolved"));
    assert_eq!(broken.usage().requests, 1);
    assert_eq!(
        broken.ledger.lock().unwrap().accounted_nanos,
        broken.backend.reservation().unwrap()
    );
    assert_eq!(
        std::fs::read_dir(other.path().join("raw")).unwrap().count(),
        0
    );
}

#[test]
fn accounted_receipt_with_unfinished_audit_stops_without_claiming_unknown_usage() {
    let dir = tempfile::tempdir().unwrap();
    let client = client(dir.path(), 1.0);
    let reservation = client.reserve().unwrap();
    let hedge = Hedge::default();
    let mut pending = PendingAttempt {
        client: &client,
        receipt_accounted: false,
        finished: false,
        reservation,
        provider: 0,
        hedge: &hedge,
        audit_name: "unfinished".into(),
    };
    client.settle(reservation, 100, 20).unwrap();
    pending.receipt_accounted = true;
    drop(pending);
    assert!(client
        .spending_stop_reason()
        .unwrap()
        .contains("audit inconsistency"));
    assert!(!client.ledger.lock().unwrap().unresolved_usage);
    assert_eq!(client.usage().input_tokens, 100);
    assert!(client.reserve().is_err());
}

#[test]
fn rejects_invalid_and_unapproved_budgets_before_auth() {
    let dir = tempfile::tempdir().unwrap();
    for budget in [0.0, -1.0, f64::NAN, 101.0] {
        let config = RunConfig {
            budget_usd: budget,
            ..RunConfig::default()
        };
        let http = HttpRecorder::new(dir.path(), &config).unwrap();
        assert!(JevClient::with_settings(
            &config,
            &http,
            |_| None,
            |_| { panic!("Test construction must not resolve credentials") }
        )
        .is_err());
    }
}

#[tokio::test]
async fn fixture_never_sends_requests_and_identifies_fixture_scores() {
    let dir = tempfile::tempdir().unwrap();
    let client = client(dir.path(), 0.0);
    let source = source();
    let route = client
        .route("How does it work?", std::slice::from_ref(&source), 0)
        .await
        .unwrap();
    assert!(route[0].reason.contains("Fixture"));
    let document = Document {
        id: "doc".into(),
        source_id: source.id.clone(),
        title: "Test".into(),
        url: String::new(),
        text: "Evidence".into(),
        provenance: Value::Null,
        raw_artifacts: vec![],
    };
    assert!(client
        .score_documents("How does it work?", std::slice::from_ref(&document))
        .await
        .pop()
        .unwrap()
        .unwrap()
        .reason
        .contains("Fixture"));
    assert_eq!(client.usage().requests, 0);
    assert_eq!(
        std::fs::read_dir(dir.path().join("raw")).unwrap().count(),
        0
    );
    assert!(client
        .route("q", &[source.clone(), source], 0)
        .await
        .is_err());
}
