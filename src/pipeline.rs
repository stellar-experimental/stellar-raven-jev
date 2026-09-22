use crate::{connectors, http::HttpRecorder, jev::JevClient, types::*};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use futures::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

const CHECKPOINT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunOutcome {
    pub directory: PathBuf,
    pub status: String,
    pub selected: usize,
    pub rejected: usize,
    pub uncertain: usize,
    pub failures: usize,
    pub usage: Usage,
}

#[derive(Default, Serialize)]
struct Evidence {
    routes: Vec<serde_json::Value>,
    source_decisions: Vec<serde_json::Value>,
    documents: Vec<Document>,
    scores: Vec<DocumentScore>,
    selected: Vec<Document>,
    rejected: Vec<Document>,
    uncertain: Vec<Document>,
    omitted: Vec<Document>,
    failures: Vec<Failure>,
    #[serde(skip)]
    timings: BTreeMap<&'static str, u64>,
}

#[async_trait]
pub trait Backend: Send + Sync {
    async fn route(
        &self,
        question: &str,
        sources: &[Source],
        pass: usize,
    ) -> Result<Vec<SourceScore>>;
    async fn fetch(
        &self,
        ctx: &FetchContext,
        source: &Source,
        question: &str,
    ) -> Result<FetchResult>;
    async fn score_document(&self, question: &str, document: &Document) -> Result<DocumentScore>;
    fn usage(&self) -> Usage;
}

struct LiveBackend {
    jev: JevClient,
}
#[async_trait]
impl Backend for LiveBackend {
    async fn route(
        &self,
        question: &str,
        sources: &[Source],
        pass: usize,
    ) -> Result<Vec<SourceScore>> {
        self.jev.route(question, sources, pass).await
    }
    async fn fetch(
        &self,
        ctx: &FetchContext,
        source: &Source,
        question: &str,
    ) -> Result<FetchResult> {
        connectors::fetch(ctx, source, question).await
    }
    async fn score_document(&self, question: &str, document: &Document) -> Result<DocumentScore> {
        self.jev.score_document(question, document).await
    }
    fn usage(&self) -> Usage {
        self.jev.usage()
    }
}

pub fn validate_config(config: &RunConfig) -> Result<()> {
    if !config.budget_usd.is_finite() || config.budget_usd < 0.0 {
        bail!("--budget-usd must be finite and nonnegative");
    }
    for (flag, value) in [
        ("timeout-secs", config.timeout_secs as usize),
        ("concurrency", config.concurrency),
        ("max-pages", config.max_pages),
        ("max-documents", config.max_documents),
        ("per-source-documents", config.per_source_documents),
        ("fetch-deadline-secs", config.fetch_deadline_secs as usize),
        ("max-body-bytes", config.max_body_bytes),
        ("route-passes", config.route_passes),
    ] {
        if value == 0 {
            bail!("--{flag} must be greater than zero");
        }
    }
    for (flag, value) in [
        ("source-threshold", config.source_threshold),
        ("document-threshold", config.document_threshold),
        ("uncertain-threshold", config.uncertain_threshold),
    ] {
        if !value.is_finite() || !(0.0..=1.0).contains(&value) {
            bail!("--{flag} must be between zero and one");
        }
    }
    if config.uncertain_threshold > config.document_threshold {
        bail!("--uncertain-threshold must not exceed --document-threshold");
    }
    Ok(())
}

fn write_json(path: impl AsRef<Path>, value: &impl Serialize) -> Result<()> {
    let path = path.as_ref();
    let temporary = path.with_extension("json.tmp");
    std::fs::write(&temporary, serde_json::to_vec_pretty(value)?)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

fn prepare(question: &str, config: &RunConfig) -> Result<(RunConfig, HttpRecorder)> {
    validate_config(config)?;
    if question.trim().is_empty() {
        bail!("The question must not be empty");
    }
    let mut config = config.clone();
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    config.output_dir = config
        .output_dir
        .join(format!("{stamp}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&config.output_dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&config.output_dir, std::fs::Permissions::from_mode(0o700))?;
    }
    write_json(
        config.output_dir.join("question.json"),
        &json!({"question": question, "config": config}),
    )?;
    let query_plan: serde_json::Value =
        serde_json::from_str(&crate::query::plan(question).to_json())?;
    write_json(config.output_dir.join("query-plan.json"), &query_plan)?;
    write_json(
        config.output_dir.join("sources.json"),
        &connectors::sources(),
    )?;
    write_json(
        config.output_dir.join("retrieved.json"),
        &Vec::<Document>::new(),
    )?;
    let http = HttpRecorder::new(&config.output_dir, &config)?;
    persist(&config, &Evidence::default(), &Usage::default(), "running")?;
    Ok((config, http))
}

fn persist(
    config: &RunConfig,
    evidence: &Evidence,
    usage: &Usage,
    status: &str,
) -> Result<RunOutcome> {
    let root = &config.output_dir;
    write_json(root.join("routes.json"), &evidence.routes)?;
    write_json(
        root.join("source-decisions.json"),
        &evidence.source_decisions,
    )?;
    write_json(root.join("documents.json"), &evidence.documents)?;
    write_json(root.join("scores.json"), &evidence.scores)?;
    write_json(root.join("selected.json"), &evidence.selected)?;
    write_json(root.join("rejected.json"), &evidence.rejected)?;
    write_json(root.join("uncertain.json"), &evidence.uncertain)?;
    write_json(root.join("omitted.json"), &evidence.omitted)?;
    write_json(root.join("failures.json"), &evidence.failures)?;
    write_json(root.join("usage.json"), usage)?;
    let outcome = RunOutcome {
        directory: root.clone(),
        status: status.into(),
        selected: evidence.selected.len(),
        rejected: evidence.rejected.len(),
        uncertain: evidence.uncertain.len(),
        failures: evidence.failures.len(),
        usage: usage.clone(),
    };
    if status != "running" {
        crate::export::write_run_index(root)?;
    }
    let mut artifacts = Vec::new();
    collect_artifacts(root, root, &mut artifacts)?;
    write_json(
        root.join("manifest.json"),
        &json!({
            "schema_version": 1, "outcome": outcome, "mode": if config.fixture { "offline-fixture" } else { "live-jev" },
            "fixture_is_model_evidence": false, "answer_generated": false, "probabilities_are_calibrated": false,
            "config": config, "artifacts": artifacts, "document_count": evidence.documents.len(),
            "scored_document_count": evidence.scores.len(), "omitted_document_count": evidence.omitted.len(),
            "phase_ms": evidence.timings,
            "completeness": "Bounded retrieval only. Inspect failures, omitted documents, source decisions, and connector provenance."
        }),
    )?;
    Ok(outcome)
}

fn collect_artifacts(
    root: &Path,
    directory: &Path,
    artifacts: &mut Vec<serde_json::Value>,
) -> Result<()> {
    let mut entries = std::fs::read_dir(directory)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_artifacts(root, &path, artifacts)?;
        } else if path.file_name().is_some_and(|name| name != "manifest.json") {
            artifacts.push(json!({"path":path.strip_prefix(root)?.to_string_lossy(),"bytes":entry.metadata()?.len()}));
        }
    }
    Ok(())
}

pub(crate) fn refresh_manifest_artifacts(root: &Path) -> Result<()> {
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("manifest.json"))?)?;
    let mut artifacts = Vec::new();
    collect_artifacts(root, root, &mut artifacts)?;
    manifest["artifacts"] = json!(artifacts);
    write_json(root.join("manifest.json"), &manifest)
}

pub async fn run_question(question: &str, config: &RunConfig) -> Result<RunOutcome> {
    run_question_scoped(question, config, connectors::SourceScope::All).await
}

pub async fn run_question_scoped(
    question: &str,
    config: &RunConfig,
    scope: connectors::SourceScope,
) -> Result<RunOutcome> {
    let (config, http) = prepare(question, config)?;
    let registry = connectors::sources();
    let sources: Vec<_> = registry
        .iter()
        .filter(|source| scope.includes(source))
        .cloned()
        .collect();
    write_json(config.output_dir.join("sources.json"), &sources)?;
    write_json(
        config.output_dir.join("source-scope.json"),
        &json!({
            "scope":scope,
            "description":scope.description(),
            "eligible_source_ids":sources.iter().map(|s| &s.id).collect::<Vec<_>>(),
            "excluded_source_ids":registry.iter().filter(|s| !scope.includes(s)).map(|s| &s.id).collect::<Vec<_>>(),
        }),
    )?;
    let jev = match JevClient::new(&config, &http) {
        Ok(jev) => jev,
        Err(error) => {
            let mut evidence = Evidence {
                source_decisions: sources
                    .iter()
                    .map(|source| {
                        json!({
                            "source_id":source.id,"max_probability":null,"selected":false,
                            "reason":"Initialization failed before routing or retrieval"
                        })
                    })
                    .collect(),
                ..Default::default()
            };
            evidence.failures.push(Failure {
                stage: "initialization".into(),
                source_id: None,
                message: error.to_string(),
            });
            return persist(&config, &evidence, &Usage::default(), "failed");
        }
    };
    execute(question, &config, http, &sources, &LiveBackend { jev }).await
}

pub async fn run_with_backend(
    question: &str,
    config: &RunConfig,
    sources: &[Source],
    backend: &dyn Backend,
) -> Result<RunOutcome> {
    let (config, http) = prepare(question, config)?;
    execute(question, &config, http, sources, backend).await
}

fn failure(stage: &str, source: Option<&str>, message: impl Into<String>) -> Failure {
    Failure {
        stage: stage.into(),
        source_id: source.map(str::to_owned),
        message: message.into(),
    }
}

async fn execute(
    question: &str,
    config: &RunConfig,
    http: HttpRecorder,
    sources: &[Source],
    backend: &dyn Backend,
) -> Result<RunOutcome> {
    let mut evidence = Evidence::default();
    let run_started = std::time::Instant::now();
    write_json(config.output_dir.join("sources.json"), &sources)?;
    let ids: BTreeSet<_> = sources.iter().map(|source| source.id.clone()).collect();
    if ids.len() != sources.len() || sources.is_empty() {
        evidence.failures.push(failure(
            "registry",
            None,
            "Source registry is empty or has duplicate IDs",
        ));
        return persist(config, &evidence, &backend.usage(), "failed");
    }
    let mut selected = BTreeSet::new();
    let mut maxima: BTreeMap<String, f64> = BTreeMap::new();
    let mut valid_passes = 0;
    // Passes are independent. Run them together, then record results in pass order.
    let routed = futures::future::join_all(
        (0..config.route_passes).map(|pass| backend.route(question, sources, pass)),
    )
    .await;
    for (pass, routed) in routed.into_iter().enumerate() {
        match routed {
            Err(error) => {
                evidence
                    .routes
                    .push(json!({"pass":pass,"error":error.to_string()}));
                evidence
                    .failures
                    .push(failure("route", None, format!("Pass {pass}: {error}")));
            }
            Ok(scores) => {
                let result = validate_routes(sources, &scores);
                evidence
                    .routes
                    .push(json!({"pass":pass,"scores":scores,"valid":result.is_ok()}));
                if let Err(error) = result {
                    evidence
                        .failures
                        .push(failure("route", None, format!("Pass {pass}: {error}")));
                    continue;
                }
                valid_passes += 1;
                for score in scores {
                    maxima
                        .entry(score.source_id.clone())
                        .and_modify(|p| *p = p.max(score.probability))
                        .or_insert(score.probability);
                    if score.probability >= config.source_threshold {
                        selected.insert(score.source_id);
                    }
                }
            }
        }
        persist(config, &evidence, &backend.usage(), "running")?;
    }
    for source in sources {
        evidence.source_decisions.push(json!({"source_id":source.id,"max_probability":maxima.get(&source.id),"selected":selected.contains(&source.id),"reason":if selected.contains(&source.id) {"selected by union across passes"} else if valid_passes == 0 {"not retrieved because all route passes failed"} else {"below source threshold in all valid passes"}}));
    }
    if valid_passes == 0 {
        return persist(config, &evidence, &backend.usage(), "failed");
    }
    evidence
        .timings
        .insert("route", run_started.elapsed().as_millis() as u64);
    let jobs = stream::iter(
        sources
            .iter()
            .filter(|source| selected.contains(&source.id)),
    )
    .map(|source| {
        let mut source_config = config.clone();
        source_config.max_documents = config.max_documents.min(config.per_source_documents);
        let ctx = FetchContext {
            http: http.clone(),
            config: source_config,
        };
        async move {
            (
                source.id.clone(),
                backend.fetch(&ctx, source, question).await,
            )
        }
    })
    .buffer_unordered(config.concurrency);
    // Heap-pinned so the stream, and every in-flight connector with its HTTP permit, can be dropped early.
    let mut jobs = Box::pin(jobs);
    let mut fetched = Vec::new();
    let mut finished = BTreeSet::new();
    let fetch_deadline =
        tokio::time::Instant::now() + std::time::Duration::from_secs(config.fetch_deadline_secs);
    loop {
        let (source_id, result) = match tokio::time::timeout_at(fetch_deadline, jobs.next()).await {
            Ok(Some(next)) => next,
            Ok(None) => break,
            Err(_) => {
                // Dropping the stream cancels the unfinished connectors. Completed raw responses remain.
                for source_id in selected.iter().filter(|id| !finished.contains(*id)) {
                    evidence.failures.push(failure("fetch_deadline", Some(source_id), format!("Connector did not finish within --fetch-deadline-secs {}; its documents were not admitted. Completed raw responses remain under raw/.", config.fetch_deadline_secs)));
                }
                break;
            }
        };
        finished.insert(source_id.clone());
        match result {
            Ok(mut result) => {
                evidence.failures.append(&mut result.failures);
                for document in &mut result.documents {
                    if document.source_id != source_id {
                        evidence.failures.push(failure(
                            "provenance",
                            Some(&source_id),
                            format!(
                                "Document {} returned a different source_id; the core corrected it",
                                document.id
                            ),
                        ));
                        document.source_id = source_id.clone();
                    }
                }
                fetched.extend(result.documents);
            }
            Err(error) => {
                evidence
                    .failures
                    .push(failure("fetch", Some(&source_id), error.to_string()))
            }
        }
    }
    drop(jobs);
    // Raw response bodies already preserve each connector's evidence while fetching runs.
    write_json(config.output_dir.join("retrieved.json"), &fetched)?;
    evidence
        .timings
        .insert("fetch", run_started.elapsed().as_millis() as u64);
    // BTreeMap gives stable source order. Each source queue preserves upstream document rank.
    // Round-robin admission gives every selected source a place before a source takes a second place.
    let mut groups: BTreeMap<String, std::collections::VecDeque<Document>> = BTreeMap::new();
    for mut document in fetched {
        // Namespace document IDs across independent connector families.
        document.id = format!("{}::{}", document.source_id, document.id);
        groups
            .entry(document.source_id.clone())
            .or_default()
            .push_back(document);
    }
    let mut seen = BTreeSet::new();
    loop {
        let mut any = false;
        for group in groups.values_mut() {
            if let Some(document) = group.pop_front() {
                any = true;
                if !seen.insert(document.id.clone()) {
                    evidence.failures.push(failure(
                        "deduplication",
                        Some(&document.source_id),
                        format!(
                            "Duplicate document ID {} retained in omitted.json",
                            document.id
                        ),
                    ));
                    evidence.omitted.push(document);
                } else if evidence.documents.len() < config.max_documents {
                    evidence.documents.push(document);
                } else {
                    evidence.omitted.push(document);
                }
            }
        }
        if !any {
            break;
        }
    }
    let mut omitted_counts: BTreeMap<&str, usize> = BTreeMap::new();
    for document in &evidence.omitted {
        *omitted_counts.entry(&document.source_id).or_default() += 1;
    }
    for (source_id, count) in omitted_counts {
        evidence.failures.push(failure("document_limit",Some(source_id),format!("Omitted {count} fetched documents from scoring; inspect omitted.json and --max-documents")));
    }
    persist(config, &evidence, &backend.usage(), "running")?;
    // Keep Jev budget and retry accounting inside JevClient. Do not cancel its paid requests externally.
    let scoring = stream::iter(evidence.documents.clone())
        .map(|document| async move {
            let result = backend.score_document(question, &document).await;
            (document, result)
        })
        .buffer_unordered(config.concurrency);
    tokio::pin!(scoring);
    // A full checkpoint rewrites every artifact. Jev audit files already record each paid attempt,
    // so checkpoint on an interval instead of after every score.
    let mut checkpoint = std::time::Instant::now();
    while let Some((document, result)) = scoring.next().await {
        match result.and_then(|score| {
            if score.document_id != document.id
                || !score.probability.is_finite()
                || !(0.0..=1.0).contains(&score.probability)
            {
                bail!("Invalid or mismatched document score");
            }
            Ok(score)
        }) {
            Ok(score) => {
                if score.probability >= config.document_threshold {
                    evidence.selected.push(document);
                } else if score.probability >= config.uncertain_threshold {
                    evidence.uncertain.push(document);
                } else {
                    evidence.rejected.push(document);
                }
                evidence.scores.push(score);
            }
            Err(error) => {
                evidence.failures.push(failure(
                    "document_score",
                    Some(&document.source_id),
                    format!("{}: {error}", document.id),
                ));
                evidence.uncertain.push(document);
            }
        }
        if checkpoint.elapsed() >= CHECKPOINT_INTERVAL {
            persist(config, &evidence, &backend.usage(), "running")?;
            checkpoint = std::time::Instant::now();
        }
    }
    evidence
        .timings
        .insert("score", run_started.elapsed().as_millis() as u64);
    evidence
        .scores
        .sort_by(|a, b| a.document_id.cmp(&b.document_id));
    for docs in [
        &mut evidence.selected,
        &mut evidence.rejected,
        &mut evidence.uncertain,
    ] {
        docs.sort_by(|a, b| a.id.cmp(&b.id));
    }
    let status = if evidence.failures.is_empty() {
        "complete"
    } else {
        "partial"
    };
    let outcome = persist(config, &evidence, &backend.usage(), status)
        .context("Could not finalize the run evidence")?;
    // Record the export cost without rewriting every artifact again.
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(config.output_dir.join("manifest.json"))?)?;
    manifest["phase_ms"]["finalize"] = json!(run_started.elapsed().as_millis() as u64);
    write_json(config.output_dir.join("manifest.json"), &manifest)?;
    Ok(outcome)
}

fn validate_routes(sources: &[Source], scores: &[SourceScore]) -> Result<()> {
    let expected: BTreeSet<_> = sources.iter().map(|s| s.id.as_str()).collect();
    let actual: BTreeSet<_> = scores.iter().map(|s| s.source_id.as_str()).collect();
    if actual != expected || actual.len() != scores.len() {
        bail!("Jev must return exactly one independent probability for every source");
    }
    if scores
        .iter()
        .any(|s| !s.probability.is_finite() || !(0.0..=1.0).contains(&s.probability))
    {
        bail!("Jev returned an invalid source probability");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };
    struct Mock {
        fetched: Mutex<Vec<String>>,
        observed_caps: Mutex<Vec<usize>>,
        scored: AtomicUsize,
        fail_fetch: bool,
        fail_score: bool,
        fail_route: bool,
        empty_fetch: bool,
        ranked: bool,
        fill_source_limit: bool,
        stall_a: bool,
    }
    #[async_trait]
    impl Backend for Mock {
        async fn route(
            &self,
            _: &str,
            sources: &[Source],
            pass: usize,
        ) -> Result<Vec<SourceScore>> {
            if self.fail_route {
                bail!("actual route failure");
            }
            Ok(sources
                .iter()
                .enumerate()
                .map(|(i, s)| SourceScore {
                    source_id: s.id.clone(),
                    probability: if i == pass { 0.9 } else { 0.05 },
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
            let mut result = FetchResult {
                documents: vec![Document {
                    id: "same-id".into(),
                    source_id: source.id.clone(),
                    title: "full".into(),
                    url: "https://example.org".into(),
                    text: "Full available document body".into(),
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
            if self.fill_source_limit {
                let template = result.documents[0].clone();
                result.documents = (0..ctx.config.max_documents)
                    .map(|index| {
                        let mut document = template.clone();
                        document.id = index.to_string();
                        document
                    })
                    .collect();
            }
            Ok(result)
        }
        async fn score_document(&self, _: &str, document: &Document) -> Result<DocumentScore> {
            self.scored.fetch_add(1, Ordering::SeqCst);
            if self.fail_score {
                bail!("actual scoring failure");
            }
            Ok(DocumentScore {
                document_id: document.id.clone(),
                probability: 0.8,
                reason: "fixture".into(),
            })
        }
        fn usage(&self) -> Usage {
            Usage::default()
        }
    }
    fn mock() -> Mock {
        Mock {
            fetched: Mutex::new(vec![]),
            observed_caps: Mutex::new(vec![]),
            scored: AtomicUsize::new(0),
            fail_fetch: false,
            fail_score: false,
            fail_route: false,
            empty_fetch: false,
            ranked: false,
            fill_source_limit: false,
            stall_a: false,
        }
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
            let retrieved: Vec<Document> = serde_json::from_slice(
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
            "selected.json",
            "rejected.json",
            "uncertain.json",
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
        let selected: Vec<Document> = serde_json::from_slice(
            &std::fs::read(outcome.directory.join("selected.json")).unwrap(),
        )
        .unwrap();
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
        let failures: Vec<Failure> = serde_json::from_slice(
            &std::fs::read(outcome.directory.join("failures.json")).unwrap(),
        )
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
        for name in [
            "retrieved.json",
            "documents.json",
            "scores.json",
            "selected.json",
            "uncertain.json",
            "rejected.json",
            "omitted.json",
            "failures.json",
        ] {
            let saved: serde_json::Value =
                serde_json::from_slice(&std::fs::read(outcome.directory.join(name)).unwrap())
                    .unwrap();
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
        let failures: Vec<Failure> = serde_json::from_slice(
            &std::fs::read(outcome.directory.join("failures.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].stage, "fetch_deadline");
        assert_eq!(failures[0].source_id.as_deref(), Some("a"));
        let selected: Vec<Document> = serde_json::from_slice(
            &std::fs::read(outcome.directory.join("selected.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(selected[0].source_id, "b");
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
        let admitted: Vec<Document> = serde_json::from_slice(
            &std::fs::read(outcome.directory.join("documents.json")).unwrap(),
        )
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
}
