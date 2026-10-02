use super::*;

/// Documents for one status, in classification order, from the single document store.
fn by_status(directory: &Path, status: &str) -> Vec<Document> {
    let read = |name: &str| std::fs::read(directory.join(name)).unwrap();
    let documents: Vec<Document> = serde_json::from_slice(&read("documents.json")).unwrap();
    let classification: serde_json::Value =
        serde_json::from_slice(&read("classification.json")).unwrap();
    classification[status]
        .as_array()
        .unwrap()
        .iter()
        .map(|id| {
            documents
                .iter()
                .find(|d| d.id == id.as_str().unwrap())
                .unwrap()
                .clone()
        })
        .collect()
}
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};
struct Mock {
    fetched: Mutex<Vec<String>>,
    observed_caps: Mutex<Vec<usize>>,
    scored: AtomicUsize,
    assessed: AtomicUsize,
    fail_fetch: bool,
    fail_score: bool,
    fail_route: bool,
    empty_fetch: bool,
    ranked: bool,
    fill_source_limit: bool,
    stall_a: bool,
    shared_url: bool,
    distinct_title: bool,
    distinct_text: bool,
    nul_collision: bool,
    /// Fixed routing probability per source index, the same in every pass.
    routes: Option<Vec<f64>>,
    /// Sources whose documents score low.
    low_sources: Vec<String>,
    /// URLs of source a's rows, each a record without its page.
    rows: Vec<String>,
    /// Test DNS for original reads.
    reader: Option<crate::http::TestDns>,
    /// Jev stops paid work: source a's scoring fails in transport, source c's and every
    /// currentness call are refused after the stop.
    stop_cascade: bool,
    /// Providers the network check took out of the run.
    skipped: BTreeMap<String, String>,
}
#[async_trait]
impl Backend for Mock {
    async fn route(&self, _: &str, sources: &[Source], pass: usize) -> Result<Vec<SourceScore>> {
        if self.fail_route {
            bail!("actual route failure");
        }
        Ok(sources
            .iter()
            .enumerate()
            .map(|(i, s)| SourceScore {
                source_id: s.id.clone(),
                probability: match &self.routes {
                    Some(routes) => routes[i],
                    None if i == pass => 0.9,
                    None => 0.05,
                },
                reason: "fixture".into(),
            })
            .collect())
    }
    async fn fetch(&self, ctx: &FetchContext, source: &Source, _: &str) -> Result<FetchResult> {
        self.fetched.lock().unwrap().push(source.id.clone());
        self.observed_caps
            .lock()
            .unwrap()
            .push(ctx.config.max_documents);
        if self.fail_fetch && source.id == "a" {
            bail!("actual connector failure");
        }
        if self.stall_a && source.id == "a" {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        }
        if self.empty_fetch {
            return Ok(FetchResult::default());
        }
        if source.id == "a" && !self.rows.is_empty() {
            return Ok(FetchResult {
                documents: self
                    .rows
                    .iter()
                    .enumerate()
                    .map(|(i, url)| Document {
                        id: format!("row-{i}"),
                        source_id: source.id.clone(),
                        title: format!("Fernlet record {i}"),
                        url: url.clone(),
                        text: format!("{{\"name\":\"fernlet {i}\"}}"),
                        provenance: json!({"content_scope":"structured_record"}),
                        raw_artifacts: vec![],
                    })
                    .collect(),
                failures: vec![],
            });
        }
        let mut result = FetchResult {
            documents: vec![Document {
                id: "same-id".into(),
                source_id: source.id.clone(),
                title: if self.distinct_title {
                    format!("full {}", source.id)
                } else {
                    "full".into()
                },
                // Distinct URLs per source unless a test asks for exact duplicates.
                url: if self.shared_url {
                    "https://example.org".into()
                } else {
                    format!("https://example.org/{}", source.id)
                },
                text: if self.distinct_text {
                    format!("Full available document body from {}", source.id)
                } else {
                    "Full available document body".into()
                },
                provenance: json!({"fixture":true}),
                raw_artifacts: vec![],
            }],
            failures: vec![],
        };
        if self.ranked {
            result.documents[0].id = "z".into();
            let mut second = result.documents[0].clone();
            second.id = "a".into();
            result.documents.push(second);
        }
        if self.nul_collision {
            // (title "a", text "b\0c") and (title "a\0b", text "c") join to the same bytes.
            let (title, text) = if source.id == "a" {
                ("a", "b\u{0}c")
            } else {
                ("a\u{0}b", "c")
            };
            result.documents[0].title = title.into();
            result.documents[0].text = text.into();
        }
        if self.fill_source_limit {
            let template = result.documents[0].clone();
            result.documents = (0..ctx.config.max_documents)
                .map(|index| {
                    let mut document = template.clone();
                    document.id = index.to_string();
                    document.url = format!("{}/{index}", template.url);
                    document
                })
                .collect();
        }
        Ok(result)
    }
    async fn score_documents(
        &self,
        question: &str,
        documents: &[Document],
    ) -> Vec<Result<DocumentScore>> {
        let mut out = Vec::new();
        for document in documents {
            out.push(self.score_one(question, document).await);
        }
        out
    }
    async fn assess_currentness(
        &self,
        _: &str,
        _: &Document,
        _: [usize; 2],
        _: Option<(&str, &str)>,
        _: &str,
    ) -> Result<f64> {
        self.assessed.fetch_add(1, Ordering::SeqCst);
        if self.stop_cascade {
            return Err(stopped());
        }
        Ok(0.9)
    }
    async fn classify_intent(&self, _: &str) -> Result<BTreeMap<String, f64>> {
        Ok(BTreeMap::from([
            ("intent=current".into(), 0.9),
            ("intent#confidence".into(), 0.9),
            ("versioned".into(), 0.9),
        ]))
    }
    fn usage(&self) -> Usage {
        Usage::default()
    }
    fn skipped_providers(&self) -> BTreeMap<String, String> {
        self.skipped.clone()
    }
    fn original_reader(&self, http: &HttpRecorder) -> Result<HttpRecorder> {
        match &self.reader {
            Some(dns) => http.public_reader_for_test(dns.clone()),
            None => http.public_reader(),
        }
    }
}
impl Mock {
    async fn score_one(&self, _: &str, document: &Document) -> Result<DocumentScore> {
        self.scored.fetch_add(1, Ordering::SeqCst);
        if self.fail_score {
            bail!("actual scoring failure");
        }
        match document.source_id.as_str() {
            "a" if self.stop_cascade => {
                return Err(crate::jev::CallFailure {
                    cause: "connection_closed".into(),
                    message: "Jev transport failed (connection_closed)".into(),
                }
                .into())
            }
            "c" if self.stop_cascade => return Err(stopped()),
            _ => {}
        }
        let probability = if self.low_sources.contains(&document.source_id) {
            0.05
        } else {
            0.8
        };
        Ok(DocumentScore {
            document_id: document.id.clone(),
            probability,
            reason: "fixture".into(),
            signals: Default::default(),
            signals_aggregation: Default::default(),
            best_chunk: [0, document.text.len()],
            usable_top2_mean: 0.8,
            still_current: None,
        })
    }
}
fn mock() -> Mock {
    Mock {
        fetched: Mutex::new(vec![]),
        observed_caps: Mutex::new(vec![]),
        scored: AtomicUsize::new(0),
        assessed: AtomicUsize::new(0),
        fail_fetch: false,
        fail_score: false,
        fail_route: false,
        empty_fetch: false,
        ranked: false,
        fill_source_limit: false,
        stall_a: false,
        shared_url: false,
        distinct_title: false,
        distinct_text: false,
        nul_collision: false,
        routes: None,
        low_sources: vec![],
        rows: vec![],
        reader: None,
        stop_cascade: false,
        skipped: BTreeMap::new(),
    }
}
fn stopped() -> anyhow::Error {
    crate::jev::Stopped("Jev stopped after unresolved paid-attempt usage".into()).into()
}
/// A page server for `fernlet.test` that counts requests, and a mock whose source a (routed
/// 0.9) returns `rows`; source b routes 0.3 and c 0.1.
async fn reading_mock(rows: &[&str]) -> (Mock, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let count = std::sync::Arc::new(AtomicUsize::new(0));
    let served = count.clone();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let served = served.clone();
            tokio::spawn(async move {
                let mut request = vec![0; 4096];
                let n = socket.read(&mut request).await.unwrap_or(0);
                served.fetch_add(1, Ordering::SeqCst);
                let path = String::from_utf8_lossy(&request[..n])
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or_default()
                    .to_owned();
                let body = format!("<html><head><title>t</title></head><body><nav>menu</nav><main><h1>Fernlet page</h1><p>Full text of {path}.</p></main></body></html>");
                let reply = format!("HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\n\r\n{body}", body.len());
                let _ = socket.write_all(reply.as_bytes()).await;
            });
        }
    });
    let mut dns = crate::http::TestDns {
        loopback: true,
        port: Some(port),
        ..Default::default()
    };
    dns.answers
        .insert("fernlet.test".into(), vec!["127.0.0.1".parse().unwrap()]);
    dns.answers.insert(
        "private.fernlet.test".into(),
        vec!["10.0.0.7".parse().unwrap()],
    );
    let mut backend = mock();
    backend.routes = Some(vec![0.9, 0.3, 0.1]);
    backend.rows = rows.iter().map(|u| (*u).to_owned()).collect();
    backend.reader = Some(dns);
    (backend, count)
}
const READ_ROWS: &[&str] = &[
    "https://fernlet.test/page-0",
    "https://fernlet.test:443/page-0#again",
    "https://private.fernlet.test/page",
    "https://fernlet.test/page-1",
    "https://fernlet.test/page-2",
    "https://fernlet.test/page-3",
];
#[tokio::test]
async fn zero_original_reads_sends_no_request_and_charges_nothing() {
    let (backend, count) = reading_mock(READ_ROWS).await;
    let dir = tempfile::tempdir().unwrap();
    let config = RunConfig {
        output_dir: dir.path().into(),
        original_reads: 0,
        ..Default::default()
    };
    let outcome = run_with_backend("question", &config, &sources(), &backend)
        .await
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 0);
    let load: serde_json::Value = read_json(&outcome.directory, "load.json").unwrap();
    assert_eq!(load["original_reads"]["attempted"], 0);
    assert_eq!(load["original_reads"]["session_charged"], 0);
}

#[tokio::test]
async fn first_pass_reads_append_scored_pages_after_existing_documents() {
    let (backend, count) = reading_mock(READ_ROWS).await;
    let dir = tempfile::tempdir().unwrap();
    let config = RunConfig {
        output_dir: dir.path().into(),
        ..Default::default()
    };
    let outcome = run_with_backend("question", &config, &sources(), &backend)
        .await
        .unwrap();
    let root = outcome.directory;
    // One URL per normalized form; four requests reserved, the fifth URL capped; the private
    // answer is refused before connect.
    assert_eq!(count.load(Ordering::SeqCst), 3);
    let load: serde_json::Value = read_json(&root, "load.json").unwrap();
    let reads = &load["original_reads"];
    assert_eq!(reads["eligible"], 5);
    assert_eq!(reads["attempted"], 4);
    assert_eq!(reads["capped"], 1);
    assert_eq!(reads["used"], 3);
    assert_eq!(reads["refused"], 1);
    assert_eq!(reads["session_charged"], 4);
    let failures: Vec<Failure> = read_json(&root, "failures.json").unwrap();
    let refused: Vec<_> = failures
        .iter()
        .filter(|f| f.stage == "original_read")
        .collect();
    assert_eq!(refused.len(), 1);
    assert!(refused[0].message.contains("private.fernlet.test"));
    assert_eq!(refused[0].source_id.as_deref(), Some("a"));
    // Fetched documents keep the positions a run without reads gives them; pages follow.
    let documents: Vec<Document> = read_json(&root, "documents.json").unwrap();
    let (fixture_backend, _) = reading_mock(READ_ROWS).await;
    let fixture_dir = tempfile::tempdir().unwrap();
    let fixture = RunConfig {
        fixture: true,
        output_dir: fixture_dir.path().into(),
        ..Default::default()
    };
    let plain = run_with_backend("question", &fixture, &sources(), &fixture_backend)
        .await
        .unwrap();
    let plain: Vec<Document> = read_json(&plain.directory, "documents.json").unwrap();
    let ids = |docs: &[Document]| docs.iter().map(|d| d.id.clone()).collect::<Vec<_>>();
    assert_eq!(ids(&documents[..plain.len()]), ids(&plain));
    let pages = &documents[plain.len()..];
    assert_eq!(pages.len(), 3);
    let scores: Vec<DocumentScore> = read_json(&root, "scores.json").unwrap();
    for page in pages {
        assert!(page.id.starts_with("original::"));
        assert_eq!(page.source_id, "original");
        assert_eq!(page.provenance["parent_source_id"], "a");
        assert!(page.provenance["parent_document_id"]
            .as_str()
            .unwrap()
            .starts_with("a::row-"));
        assert_eq!(page.provenance["full_original"], true);
        assert_eq!(page.provenance["content_scope"], "main_visible_text");
        assert_eq!(page.provenance["fetched_url"], page.url);
        assert!(page.text.contains("Full text of /page-") && !page.text.contains("menu"));
        assert!(scores.iter().any(|s| s.document_id == page.id));
    }
    assert_eq!(pages[0].url, "https://fernlet.test/page-0");
    let retrieved: Vec<serde_json::Value> = read_json(&root, "retrieved.json").unwrap();
    assert_eq!(
        retrieved
            .iter()
            .filter(|r| r["source_id"] == "original")
            .count(),
        3
    );
}
#[tokio::test]
async fn fixture_mode_reads_no_original_pages() {
    let (backend, count) = reading_mock(READ_ROWS).await;
    let dir = tempfile::tempdir().unwrap();
    let config = RunConfig {
        fixture: true,
        output_dir: dir.path().into(),
        ..Default::default()
    };
    let outcome = run_with_backend("question", &config, &sources(), &backend)
        .await
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 0);
    let documents: Vec<Document> = read_json(&outcome.directory, "documents.json").unwrap();
    assert!(documents.iter().all(|d| d.source_id != "original"));
    let load: serde_json::Value = read_json(&outcome.directory, "load.json").unwrap();
    assert!(load["original_reads"].is_null());
    assert!(!outcome.directory.join("session.json").exists());
}
#[tokio::test]
async fn a_host_with_a_fetch_slot_cap_is_not_read_as_an_original() {
    let (backend, _) =
        reading_mock(&["https://capped.test/page", "https://fernlet.test/page"]).await;
    let dir = tempfile::tempdir().unwrap();
    let config = RunConfig {
        output_dir: dir.path().into(),
        source_slots: BTreeMap::from([("capped.test".to_owned(), 1)]),
        ..Default::default()
    };
    let (config, http) = prepare("question", &config).unwrap();
    let mut evidence = Evidence::default();
    let rows = backend
        .fetch(
            &FetchContext {
                http,
                config: config.clone(),
                deadline: None,
            },
            &sources()[0],
            "q",
        )
        .await
        .unwrap()
        .documents;
    let rows = admit(&config, &mut evidence, rows);
    let routes = BTreeMap::from([("a".to_owned(), 0.9)]);
    let plan = plan_originals(&config, &evidence, &rows, &routes, &sources()).unwrap();
    let hosts: Vec<_> = plan.reads.iter().map(|c| c.url.host_str()).collect();
    assert_eq!(hosts, [Some("fernlet.test")]);
    assert_eq!((plan.eligible, plan.slot_host_skipped), (1, 1));
}
#[tokio::test]
async fn a_page_the_session_holds_is_reused_without_a_request() {
    let (backend, count) = reading_mock(&["https://fernlet.test/held#part"]).await;
    let dir = tempfile::tempdir().unwrap();
    let config = RunConfig {
        output_dir: dir.path().into(),
        ..Default::default()
    };
    let (config, http) = prepare("question", &config).unwrap();
    let mut evidence = Evidence::default();
    evidence.documents.push(Document {
        id: "b::page".into(),
        source_id: "b".into(),
        title: "Held page".into(),
        url: "https://fernlet.test/held".into(),
        text: "Held body".into(),
        provenance: json!({"content_scope":"main_visible_text","original":{"full_original":true}}),
        raw_artifacts: vec![],
    });
    let rows = backend
        .fetch(
            &FetchContext {
                http: http.clone(),
                config: config.clone(),
                deadline: None,
            },
            &sources()[0],
            "q",
        )
        .await
        .unwrap()
        .documents;
    let rows = admit(&config, &mut evidence, rows);
    let routes = BTreeMap::from([("a".to_owned(), 0.9)]);
    let plan = plan_originals(&config, &evidence, &rows, &routes, &sources()).unwrap();
    assert!(plan.reads.is_empty());
    assert_eq!((plan.eligible, plan.reused), (1, 1));
    let read = read_originals(&backend, &http, &plan.reads).await;
    add_originals("q", &config, &backend, &mut evidence, plan, read)
        .await
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(evidence.original_reads["reused"], 1);
    assert_eq!(evidence.original_reads["session_charged"], 0);
}
fn sources() -> Vec<Source> {
    ["a", "b", "c"]
        .iter()
        .map(|id| Source {
            id: id.to_string(),
            name: id.to_string(),
            description: "test".into(),
            family: "test".into(),
        })
        .collect()
}
#[tokio::test]
async fn score_depth_scores_the_first_documents_and_keeps_unpromising_tails_unscored() {
    let dir = tempfile::tempdir().unwrap();
    let mut backend = mock();
    backend.fill_source_limit = true;
    // a routes high; b routes low but its first document scores well; c does neither.
    backend.routes = Some(vec![0.9, 0.3, 0.3]);
    backend.low_sources = vec!["c".into()];
    let config = RunConfig {
        fixture: true,
        output_dir: dir.path().into(),
        per_source_documents: 4,
        score_depth: 1,
        ..Default::default()
    };
    let outcome = run_with_backend("question", &config, &sources(), &backend)
        .await
        .unwrap();
    assert_eq!(backend.scored.load(Ordering::SeqCst), 9);
    let deferred: Vec<Document> = read_json(&outcome.directory, "deferred.json").unwrap();
    assert_eq!(deferred.len(), 3);
    assert!(deferred.iter().all(|d| d.source_id == "c"));
    let evidence = load_evidence(&outcome.directory, &config).unwrap();
    let (tails, unfetched) = open_pools(&evidence);
    assert_eq!(tails, BTreeMap::from([("c".to_owned(), 3)]));
    assert!(unfetched.is_empty());
}
#[tokio::test]
async fn a_fetch_threshold_keeps_low_routed_sources_as_unfetched_pools() {
    let dir = tempfile::tempdir().unwrap();
    let mut backend = mock();
    backend.routes = Some(vec![0.9, 0.3, 0.1]);
    let config = RunConfig {
        fixture: true,
        output_dir: dir.path().into(),
        fetch_threshold: 0.4,
        ..Default::default()
    };
    let outcome = run_with_backend("question", &config, &sources(), &backend)
        .await
        .unwrap();
    assert_eq!(*backend.fetched.lock().unwrap(), vec!["a".to_owned()]);
    let evidence = load_evidence(&outcome.directory, &config).unwrap();
    let (tails, unfetched) = open_pools(&evidence);
    assert!(tails.is_empty());
    // b routed above the source threshold, c below it.
    assert_eq!(unfetched, vec!["b".to_owned()]);
}
#[tokio::test]
async fn a_later_copy_of_a_scored_document_reuses_relevance_but_not_currentness() {
    let dir = tempfile::tempdir().unwrap();
    let backend = mock();
    let config = RunConfig {
        fixture: true,
        output_dir: dir.path().into(),
        ..Default::default()
    };
    let document = |id: &str, source: &str| Document {
        id: id.into(),
        source_id: source.into(),
        title: "Same".into(),
        url: "https://example.org/same".into(),
        text: "Same text".into(),
        provenance: json!({}),
        raw_artifacts: vec![],
    };
    let mut evidence = Evidence::default();
    score_batch(
        "q",
        &config,
        &backend,
        &mut evidence,
        vec![document("a::1", "a")],
    )
    .await
    .unwrap();
    evidence.scores[0].still_current = Some(0.9);
    score_batch(
        "q",
        &config,
        &backend,
        &mut evidence,
        vec![document("b::1", "b")],
    )
    .await
    .unwrap();
    assert_eq!(backend.scored.load(Ordering::SeqCst), 1);
    let copy = evidence
        .scores
        .iter()
        .find(|s| s.document_id == "b::1")
        .unwrap();
    assert_eq!(copy.probability, 0.8);
    assert_eq!(copy.still_current, None);
}
#[tokio::test]
async fn documents_left_unscored_by_a_stopped_call_return_to_the_pool() {
    let dir = tempfile::tempdir().unwrap();
    let backend = mock();
    let config = RunConfig {
        fixture: true,
        output_dir: dir.path().into(),
        ..Default::default()
    };
    let outcome = run_with_backend("question", &config, &sources(), &backend)
        .await
        .unwrap();
    let root = outcome.directory;
    let mut documents: Vec<Document> = read_json(&root, "documents.json").unwrap();
    let mut unfinished = documents[0].clone();
    unfinished.id = "a::unfinished".into();
    documents.push(unfinished);
    write_json(root.join("documents.json"), &documents).unwrap();
    let position = documents.len();
    let mut evidence = load_evidence(&root, &config).unwrap();
    assert_eq!(evidence.deferred.len(), 1);
    assert_eq!(evidence.deferred[0].id, "a::unfinished");
    // Scoring it later keeps its position and adds no second copy.
    let pending = std::mem::take(&mut evidence.deferred);
    score_batch("question", &config, &backend, &mut evidence, pending)
        .await
        .unwrap();
    assert_eq!(evidence.documents.len(), position);
    assert_eq!(evidence.documents[position - 1].id, "a::unfinished");
}
#[tokio::test]
async fn recovery_drops_finished_documents_from_the_pool_and_classifies_saved_scores() {
    let dir = tempfile::tempdir().unwrap();
    let backend = mock();
    let config = RunConfig {
        fixture: true,
        output_dir: dir.path().into(),
        ..Default::default()
    };
    let outcome = run_with_backend("question", &config, &sources(), &backend)
        .await
        .unwrap();
    let root = outcome.directory;
    // A stopped call left a classified document in the pool, and a scored document without
    // its classification.
    let documents: Vec<Document> = read_json(&root, "documents.json").unwrap();
    write_json(root.join("deferred.json"), &vec![documents[0].clone()]).unwrap();
    let mut classification: serde_json::Value = read_json(&root, "classification.json").unwrap();
    let unclassified = documents[1].id.clone();
    for status in ["selected", "uncertain", "rejected"] {
        classification[status]
            .as_array_mut()
            .unwrap()
            .retain(|id| id.as_str() != Some(unclassified.as_str()));
    }
    write_json(root.join("classification.json"), &classification).unwrap();
    let evidence = load_evidence(&root, &config).unwrap();
    assert!(evidence.deferred.is_empty());
    assert!(evidence.selected.iter().any(|d| d.id == unclassified));
    assert_eq!(evidence.scores.len(), documents.len());
}
#[tokio::test]
async fn per_source_limit_bounds_retrieval_without_reducing_global_scoring_capacity() {
    for (global, per_source, local, scored) in [(400, 4, 4, 12), (2, 4, 2, 2)] {
        let dir = tempfile::tempdir().unwrap();
        let mut backend = mock();
        backend.fill_source_limit = true;
        let config = RunConfig {
            fixture: true,
            output_dir: dir.path().into(),
            route_passes: 3,
            max_documents: global,
            per_source_documents: per_source,
            ..Default::default()
        };
        let outcome = run_with_backend("question", &config, &sources(), &backend)
            .await
            .unwrap();
        assert_eq!(*backend.observed_caps.lock().unwrap(), vec![local; 3]);
        assert_eq!(outcome.selected, scored);
        assert_eq!(backend.scored.load(Ordering::SeqCst), scored);
        let manifest: serde_json::Value = serde_json::from_slice(
            &std::fs::read(outcome.directory.join("manifest.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(manifest["config"]["max_documents"], global);
        assert_eq!(manifest["config"]["per_source_documents"], per_source);
        let retrieved: Vec<serde_json::Value> = serde_json::from_slice(
            &std::fs::read(outcome.directory.join("retrieved.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(retrieved.len(), local * 3);
    }
}
#[tokio::test]
async fn unions_passes_fetches_all_selected_sources_and_scores_every_document() {
    let dir = tempfile::tempdir().unwrap();
    let backend = mock();
    let config = RunConfig {
        fixture: true,
        output_dir: dir.path().into(),
        ..Default::default()
    };
    let outcome = run_with_backend("question", &config, &sources(), &backend)
        .await
        .unwrap();
    assert_eq!(outcome.status, "complete");
    assert_eq!(outcome.selected, 2);
    assert_eq!(backend.fetched.lock().unwrap().len(), 2);
    assert_eq!(backend.scored.load(Ordering::SeqCst), 2);
    for path in [
        "manifest.json",
        "routes.json",
        "source-decisions.json",
        "documents.json",
        "scores.json",
        "classification.json",
        "failures.json",
        "usage.json",
    ] {
        assert!(outcome.directory.join(path).exists());
    }
}
#[tokio::test]
async fn connector_failure_preserves_other_sources_and_scoring_failure_is_uncertain() {
    let dir = tempfile::tempdir().unwrap();
    let mut backend = mock();
    backend.fail_fetch = true;
    backend.fail_score = true;
    let config = RunConfig {
        fixture: true,
        output_dir: dir.path().into(),
        ..Default::default()
    };
    let outcome = run_with_backend("question", &config, &sources(), &backend)
        .await
        .unwrap();
    assert_eq!(outcome.status, "partial");
    assert_eq!(outcome.uncertain, 1);
    assert_eq!(outcome.failures, 2);
    assert_eq!(backend.fetched.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn failing_source_preserves_successful_sibling_and_its_score() {
    let dir = tempfile::tempdir().unwrap();
    let mut backend = mock();
    backend.fail_fetch = true;
    let config = RunConfig {
        fixture: true,
        output_dir: dir.path().into(),
        ..Default::default()
    };
    let outcome = run_with_backend("question", &config, &sources(), &backend)
        .await
        .unwrap();
    assert_eq!(outcome.status, "partial");
    assert_eq!(outcome.selected, 1);
    assert_eq!(outcome.uncertain, 0);
    assert_eq!(outcome.rejected, 0);
    assert_eq!(outcome.failures, 1);
    assert_eq!(backend.scored.load(Ordering::SeqCst), 1);
    let fetched: BTreeSet<_> = backend.fetched.lock().unwrap().iter().cloned().collect();
    assert_eq!(fetched, BTreeSet::from(["a".into(), "b".into()]));
    let selected: Vec<Document> = by_status(&outcome.directory, "selected");
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].source_id, "b");
    assert_eq!(selected[0].id, "b::same-id");
    assert_eq!(selected[0].text, "Full available document body");
    assert_eq!(selected[0].provenance, json!({"fixture":true}));
    let scores: Vec<DocumentScore> =
        serde_json::from_slice(&std::fs::read(outcome.directory.join("scores.json")).unwrap())
            .unwrap();
    assert_eq!(scores.len(), 1);
    assert_eq!(scores[0].document_id, selected[0].id);
    assert_eq!(scores[0].probability, 0.8);
    let failures: Vec<Failure> =
        serde_json::from_slice(&std::fs::read(outcome.directory.join("failures.json")).unwrap())
            .unwrap();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].source_id.as_deref(), Some("a"));
    assert_eq!(failures[0].stage, "fetch");
    assert!(failures[0].message.contains("actual connector failure"));
}
#[tokio::test]
async fn empty_success_fetches_selected_sources_without_scoring() {
    let dir = tempfile::tempdir().unwrap();
    let mut backend = mock();
    backend.empty_fetch = true;
    let config = RunConfig {
        fixture: true,
        output_dir: dir.path().into(),
        ..Default::default()
    };
    let outcome = run_with_backend("question", &config, &sources(), &backend)
        .await
        .unwrap();
    assert_eq!(outcome.status, "complete");
    assert_eq!(outcome.failures, 0);
    assert_eq!(outcome.selected, 0);
    assert_eq!(outcome.uncertain, 0);
    assert_eq!(outcome.rejected, 0);
    assert_eq!(backend.scored.load(Ordering::SeqCst), 0);
    let fetched: BTreeSet<_> = backend.fetched.lock().unwrap().iter().cloned().collect();
    assert_eq!(fetched, BTreeSet::from(["a".into(), "b".into()]));
    let classification: serde_json::Value = serde_json::from_slice(
        &std::fs::read(outcome.directory.join("classification.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        classification,
        json!({"selected":[],"uncertain":[],"rejected":[]})
    );
    for name in [
        "retrieved.json",
        "documents.json",
        "scores.json",
        "omitted.json",
        "failures.json",
    ] {
        let saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(outcome.directory.join(name)).unwrap()).unwrap();
        assert_eq!(saved, json!([]), "{name} must remain empty");
    }
    let decisions: serde_json::Value = serde_json::from_slice(
        &std::fs::read(outcome.directory.join("source-decisions.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        decisions
            .as_array()
            .unwrap()
            .iter()
            .filter(|decision| decision["selected"] == true)
            .count(),
        2
    );
}
#[tokio::test]
async fn fetch_deadline_drops_stalled_connector_and_keeps_finished_sources() {
    let dir = tempfile::tempdir().unwrap();
    let mut backend = mock();
    backend.stall_a = true;
    let config = RunConfig {
        fixture: true,
        output_dir: dir.path().into(),
        fetch_deadline_secs: 1,
        ..Default::default()
    };
    let outcome = run_with_backend("question", &config, &sources(), &backend)
        .await
        .unwrap();
    assert_eq!(outcome.status, "partial");
    assert_eq!(outcome.selected, 1);
    let failures: Vec<Failure> =
        serde_json::from_slice(&std::fs::read(outcome.directory.join("failures.json")).unwrap())
            .unwrap();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].stage, "fetch_deadline");
    assert_eq!(failures[0].source_id.as_deref(), Some("a"));
    let selected: Vec<Document> = by_status(&outcome.directory, "selected");
    assert_eq!(selected[0].source_id, "b");
}
#[test]
fn jev_batch_must_be_between_one_and_eight() {
    for (batch, ok) in [(0, false), (1, true), (8, true), (9, false)] {
        let config = RunConfig {
            jev_batch: batch,
            ..Default::default()
        };
        assert_eq!(validate_config(&config).is_ok(), ok, "{batch}");
    }
}
#[tokio::test]
async fn exact_duplicates_score_once_and_fan_back_to_every_id() {
    let dir = tempfile::tempdir().unwrap();
    let mut backend = mock();
    backend.shared_url = true;
    let config = RunConfig {
        fixture: true,
        output_dir: dir.path().into(),
        ..Default::default()
    };
    // Sources a and b both return an identical document at the same URL.
    let outcome = run_with_backend("question", &config, &sources(), &backend)
        .await
        .unwrap();
    assert_eq!(outcome.status, "complete");
    assert_eq!(outcome.selected, 2);
    assert_eq!(backend.scored.load(Ordering::SeqCst), 1);
    let scores: Vec<DocumentScore> =
        serde_json::from_slice(&std::fs::read(outcome.directory.join("scores.json")).unwrap())
            .unwrap();
    assert_eq!(scores.len(), 2);
    let copied = scores
        .iter()
        .find(|s| s.reason.starts_with("Score copied from"))
        .unwrap();
    assert_eq!(copied.probability, 0.8);
    assert!(copied.reason.contains("a::same-id"));
    assert_eq!(copied.document_id, "b::same-id");
    // The current intent asks currentness once for the identical pair and copies the answer.
    assert_eq!(backend.assessed.load(Ordering::SeqCst), 1);
    assert!(scores.iter().all(|s| s.still_current == Some(0.9)));
}
#[tokio::test]
async fn same_url_with_different_title_or_text_is_scored_separately() {
    // Jev sees the title with the text, so a different title is different scoring input.
    for (distinct_title, distinct_text) in [(true, false), (false, true)] {
        let dir = tempfile::tempdir().unwrap();
        let mut backend = mock();
        backend.shared_url = true;
        backend.distinct_title = distinct_title;
        backend.distinct_text = distinct_text;
        let config = RunConfig {
            fixture: true,
            output_dir: dir.path().into(),
            ..Default::default()
        };
        let outcome = run_with_backend("question", &config, &sources(), &backend)
            .await
            .unwrap();
        assert_eq!(outcome.selected, 2);
        assert_eq!(backend.scored.load(Ordering::SeqCst), 2);
        let scores: Vec<DocumentScore> =
            serde_json::from_slice(&std::fs::read(outcome.directory.join("scores.json")).unwrap())
                .unwrap();
        assert!(scores.iter().all(|s| !s.reason.starts_with("Score copied")));
    }
}
#[tokio::test]
async fn dedup_key_is_not_fooled_by_separator_bytes_inside_title_or_text() {
    let dir = tempfile::tempdir().unwrap();
    let mut backend = mock();
    backend.shared_url = true;
    backend.nul_collision = true;
    let config = RunConfig {
        fixture: true,
        output_dir: dir.path().into(),
        ..Default::default()
    };
    let outcome = run_with_backend("question", &config, &sources(), &backend)
        .await
        .unwrap();
    assert_eq!(outcome.selected, 2);
    assert_eq!(backend.scored.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn items_refused_after_a_stop_are_not_assessed_not_failed() {
    let dir = tempfile::tempdir().unwrap();
    let mut backend = mock();
    backend.routes = Some(vec![0.9, 0.9, 0.9]);
    backend.stop_cascade = true;
    // The run also went on without one provider that failed its network check.
    backend.skipped = BTreeMap::from([("fernlet".into(), "connect_denied".into())]);
    let config = RunConfig {
        fixture: true,
        output_dir: dir.path().into(),
        ..Default::default()
    };
    let outcome = run_with_backend("question", &config, &sources(), &backend)
        .await
        .unwrap();
    // b's document was selected, then its currentness call was refused after the stop.
    assert_eq!(backend.assessed.load(Ordering::SeqCst), 1);
    let failures: Vec<Failure> = read_json(&outcome.directory, "failures.json").unwrap();
    let rows: Vec<(&str, Option<&str>, Option<&str>)> = failures
        .iter()
        .map(|f| (f.stage.as_str(), f.source_id.as_deref(), f.cause.as_deref()))
        .collect();
    assert_eq!(
        rows,
        [
            ("document_score", Some("a"), Some("connection_closed")),
            (NOT_ASSESSED_AFTER_STOP, Some("c"), None),
            (NOT_ASSESSED_AFTER_STOP, None, None),
        ]
    );
    assert!(failures[1]
        .message
        .contains("document_score not assessed: Jev stopped"));
    assert!(failures[2]
        .message
        .contains("currentness not assessed: Jev stopped"));
    // Only a scoped run records its source scope; the report reads it.
    write_json(
        outcome.directory.join("source-scope.json"),
        &json!({"scope":"all"}),
    )
    .unwrap();
    let report = crate::search::build_report_variant(&outcome, false, None).unwrap();
    let load = &crate::search::compact_report(&report, 10)["load"];
    assert_eq!(load["scoring_failures"], 1);
    assert_eq!(load["currentness_failures"], 0);
    assert_eq!(load["not_assessed_after_stop"], 2);
    assert_eq!(load["jev_failure_causes"], json!({"connection_closed": 1}));
    assert_eq!(
        load["jev_providers_skipped"],
        json!({"fernlet": "connect_denied"})
    );
    assert_eq!(load["degraded"], true);
}
#[tokio::test]
async fn failed_representative_leaves_every_duplicate_uncertain_with_its_own_failure() {
    let dir = tempfile::tempdir().unwrap();
    let mut backend = mock();
    backend.shared_url = true;
    backend.fail_score = true;
    let config = RunConfig {
        fixture: true,
        output_dir: dir.path().into(),
        ..Default::default()
    };
    let outcome = run_with_backend("question", &config, &sources(), &backend)
        .await
        .unwrap();
    assert_eq!(backend.scored.load(Ordering::SeqCst), 1);
    assert_eq!(outcome.uncertain, 2);
    assert_eq!(outcome.selected, 0);
    // One failure row per original ID, each naming its own source.
    assert_eq!(outcome.failures, 2);
    let failures: Vec<Failure> =
        serde_json::from_slice(&std::fs::read(outcome.directory.join("failures.json")).unwrap())
            .unwrap();
    let failed_sources: BTreeSet<_> = failures
        .iter()
        .filter(|f| f.stage == "document_score")
        .filter_map(|f| f.source_id.as_deref())
        .collect();
    assert_eq!(failed_sources, BTreeSet::from(["a", "b"]));
    let uncertain: Vec<Document> = by_status(&outcome.directory, "uncertain");
    let sources_seen: BTreeSet<_> = uncertain.iter().map(|d| d.source_id.as_str()).collect();
    assert_eq!(sources_seen, BTreeSet::from(["a", "b"]));
    assert!(uncertain
        .iter()
        .all(|d| d.provenance == json!({"fixture":true})));
}
#[tokio::test]
async fn route_failure_does_not_silently_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let mut backend = mock();
    backend.fail_route = true;
    let config = RunConfig {
        fixture: true,
        output_dir: dir.path().into(),
        ..Default::default()
    };
    let outcome = run_with_backend("question", &config, &sources(), &backend)
        .await
        .unwrap();
    assert_eq!(outcome.status, "failed");
    assert!(backend.fetched.lock().unwrap().is_empty());
    assert!(outcome.directory.join("manifest.json").exists());
}
#[tokio::test]
async fn global_document_limit_retains_omissions_and_reports_each_lane() {
    let dir = tempfile::tempdir().unwrap();
    let backend = mock();
    let config = RunConfig {
        fixture: true,
        output_dir: dir.path().into(),
        max_documents: 1,
        ..Default::default()
    };
    let outcome = run_with_backend("question", &config, &sources(), &backend)
        .await
        .unwrap();
    assert_eq!(outcome.status, "partial");
    assert_eq!(outcome.selected, 1);
    assert_eq!(backend.scored.load(Ordering::SeqCst), 1);
    let omitted: Vec<Document> =
        serde_json::from_slice(&std::fs::read(outcome.directory.join("omitted.json")).unwrap())
            .unwrap();
    assert_eq!(omitted.len(), 1);
    assert_eq!(omitted[0].source_id, "b");
}
#[tokio::test]
async fn document_cap_preserves_upstream_rank_in_each_source() {
    let dir = tempfile::tempdir().unwrap();
    let mut backend = mock();
    backend.ranked = true;
    let config = RunConfig {
        fixture: true,
        output_dir: dir.path().into(),
        max_documents: 2,
        ..Default::default()
    };
    let outcome = run_with_backend("question", &config, &sources(), &backend)
        .await
        .unwrap();
    let admitted: Vec<Document> =
        serde_json::from_slice(&std::fs::read(outcome.directory.join("documents.json")).unwrap())
            .unwrap();
    assert_eq!(
        admitted.iter().map(|d| d.id.as_str()).collect::<Vec<_>>(),
        vec!["a::z", "b::z"]
    );
    let omitted: Vec<Document> =
        serde_json::from_slice(&std::fs::read(outcome.directory.join("omitted.json")).unwrap())
            .unwrap();
    assert_eq!(
        omitted.iter().map(|d| d.id.as_str()).collect::<Vec<_>>(),
        vec!["a::a", "b::a"]
    );
}
#[test]
fn rejects_missing_duplicate_and_nonfinite_route_probabilities() {
    assert!(validate_routes(&sources(), &[]).is_err());
    let scores = sources()
        .iter()
        .map(|s| SourceScore {
            source_id: s.id.clone(),
            probability: f64::NAN,
            reason: String::new(),
        })
        .collect::<Vec<_>>();
    assert!(validate_routes(&sources(), &scores).is_err());
}
