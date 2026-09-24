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
    /// For a `busy` run: how long until the host expects room for this question.
    pub retry_after_ms: Option<u64>,
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
    /// The question time intent. See `rank`.
    intent: serde_json::Value,
    /// Source load counters from the HTTP recorder. See `HttpRecorder::load_summary`.
    load: serde_json::Value,
    /// Set when the question found no room in a source's request window in time.
    #[serde(skip)]
    retry_after_ms: Option<u64>,
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
    /// Scores in input order, one per document.
    async fn score_documents(
        &self,
        question: &str,
        documents: &[Document],
    ) -> Vec<Result<DocumentScore>>;
    async fn classify_intent(&self, question: &str) -> Result<BTreeMap<String, f64>>;
    async fn assess_currentness(
        &self,
        question: &str,
        document: &Document,
        chunk: [usize; 2],
        date: Option<(&str, &str)>,
        today: &str,
    ) -> Result<f64>;
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
    async fn score_documents(
        &self,
        question: &str,
        documents: &[Document],
    ) -> Vec<Result<DocumentScore>> {
        self.jev.score_documents(question, documents).await
    }
    async fn classify_intent(&self, question: &str) -> Result<BTreeMap<String, f64>> {
        self.jev.classify_intent(question).await
    }
    async fn assess_currentness(
        &self,
        question: &str,
        document: &Document,
        chunk: [usize; 2],
        date: Option<(&str, &str)>,
        today: &str,
    ) -> Result<f64> {
        self.jev
            .assess_currentness(question, document, chunk, date, today)
            .await
    }
    fn usage(&self) -> Usage {
        self.jev.usage()
    }
}

pub fn validate_config(config: &RunConfig) -> Result<()> {
    if config.jev_batch > 8 {
        bail!("--jev-batch must be 8 or less");
    }
    if crate::rank::iso_date_days(&config.today).is_none() {
        bail!("--today must be a calendar date in YYYY-MM-DD form");
    }
    if !config.budget_usd.is_finite() || config.budget_usd < 0.0 {
        bail!("--budget-usd must be finite and nonnegative");
    }
    for (flag, value) in [
        ("timeout-secs", config.timeout_secs as usize),
        ("concurrency", config.concurrency),
        ("jev-concurrency", config.jev_concurrency),
        ("jev-batch", config.jev_batch),
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
    std::fs::write(&temporary, serde_json::to_vec(value)?)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

/// Fetch order without text: the text of every fetched document is in documents.json or
/// omitted.json under the namespaced ID `source_id::id`.
fn retrieved_index(fetched: &[Document]) -> Vec<serde_json::Value> {
    fetched
        .iter()
        .map(|d| json!({"id":d.id,"source_id":d.source_id}))
        .collect()
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
        &Vec::<serde_json::Value>::new(),
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
    let outcome = RunOutcome {
        directory: root.clone(),
        status: status.into(),
        selected: evidence.selected.len(),
        rejected: evidence.rejected.len(),
        uncertain: evidence.uncertain.len(),
        failures: evidence.failures.len(),
        usage: usage.clone(),
        retry_after_ms: evidence.retry_after_ms,
    };
    // A light record keeps no mid-run checkpoints; the final write feeds the report.
    if status == "running" && !config.full_record {
        return Ok(outcome);
    }
    write_json(root.join("routes.json"), &evidence.routes)?;
    write_json(
        root.join("source-decisions.json"),
        &evidence.source_decisions,
    )?;
    write_json(root.join("documents.json"), &evidence.documents)?;
    write_json(root.join("scores.json"), &evidence.scores)?;
    // Each admitted document's text is stored once, in documents.json. The classification
    // keeps each status list in its original order by ID.
    let ids = |documents: &[Document]| documents.iter().map(|d| d.id.clone()).collect::<Vec<_>>();
    write_json(
        root.join("classification.json"),
        &json!({"selected":ids(&evidence.selected),"uncertain":ids(&evidence.uncertain),"rejected":ids(&evidence.rejected)}),
    )?;
    write_json(root.join("omitted.json"), &evidence.omitted)?;
    write_json(root.join("failures.json"), &evidence.failures)?;
    write_json(root.join("usage.json"), usage)?;
    write_json(root.join("intent.json"), &evidence.intent)?;
    write_json(root.join("load.json"), &evidence.load)?;
    let artifacts = collect_artifacts(root)?;
    write_json(
        root.join("manifest.json"),
        &json!({
            "schema_version": 1, "outcome": outcome, "mode": if config.fixture { "offline-fixture" } else { "live-jev" },
            "record": if config.full_record { "full" } else { "light" },
            "fixture_is_model_evidence": false, "answer_generated": false, "probabilities_are_calibrated": false,
            "config": config, "artifacts": artifacts, "document_count": evidence.documents.len(),
            "scored_document_count": evidence.scores.len(), "omitted_document_count": evidence.omitted.len(),
            "phase_ms": evidence.timings,
            "completeness": "Bounded retrieval only. Inspect failures, omitted documents, source decisions, and connector provenance."
        }),
    )?;
    Ok(outcome)
}

/// Top-level files with their sizes, and a file count and byte total per directory.
fn collect_artifacts(root: &Path) -> Result<serde_json::Value> {
    let mut files = Vec::new();
    let mut directories = serde_json::Map::new();
    let mut entries = std::fs::read_dir(root)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let name = entry.file_name().to_string_lossy().into_owned();
        if entry.path().is_dir() {
            let (mut count, mut bytes) = (0u64, 0u64);
            let mut stack = vec![entry.path()];
            while let Some(directory) = stack.pop() {
                for child in std::fs::read_dir(directory)? {
                    let child = child?;
                    if child.path().is_dir() {
                        stack.push(child.path());
                    } else {
                        count += 1;
                        bytes += child.metadata()?.len();
                    }
                }
            }
            directories.insert(name, json!({"files":count,"bytes":bytes}));
        } else if name != "manifest.json" {
            files.push(json!({"path":name,"bytes":entry.metadata()?.len()}));
        }
    }
    Ok(json!({"files":files,"directories":directories}))
}

/// The default light record: keep only manifest.json, search.json, and the search-documents
/// text files that the report's text_path fields name. Run after the report is built.
pub fn keep_report_only(root: &Path) -> Result<()> {
    anyhow::ensure!(
        root.join("search.json").is_file() && root.join("manifest.json").is_file(),
        "The report must exist before the run folder is reduced to it"
    );
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        if matches!(
            name.to_str(),
            Some("manifest.json" | "search.json" | "search-documents")
        ) {
            continue;
        }
        if entry.path().is_dir() {
            std::fs::remove_dir_all(entry.path())?;
        } else {
            std::fs::remove_file(entry.path())?;
        }
    }
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("manifest.json"))?)?;
    manifest["record"] = json!("light");
    manifest["artifacts"] = collect_artifacts(root)?;
    write_json(root.join("manifest.json"), &manifest)
}

pub(crate) fn refresh_manifest_artifacts(root: &Path) -> Result<()> {
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("manifest.json"))?)?;
    manifest["artifacts"] = collect_artifacts(root)?;
    write_json(root.join("manifest.json"), &manifest)
}

pub async fn run_question_scoped(
    question: &str,
    config: &RunConfig,
    scope: connectors::SourceScope,
) -> Result<RunOutcome> {
    let (config, http) = prepare(question, config)?;
    let governor = match &config.host_dir {
        Some(dir) => crate::governor::Governor::at(dir)?,
        None => crate::governor::Governor::local(),
    };
    let http = http.with_source_gates(std::sync::Arc::new(governor));
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
    let jev = match JevClient::new(&config, &http.with_concurrency(config.jev_concurrency)) {
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
    // The intent question is independent of routing, so it runs alongside the routing passes.
    let (routed, intent) = futures::join!(
        futures::future::join_all(
            (0..config.route_passes).map(|pass| backend.route(question, sources, pass)),
        ),
        backend.classify_intent(question)
    );
    let intent = match intent {
        Ok(answers) => crate::rank::Intent::from_answers(&answers),
        Err(error) => {
            evidence
                .failures
                .push(failure("intent", None, error.to_string()));
            crate::rank::Intent {
                kind: "timeless".into(),
                confidence: 0.0,
                versioned: 0.0,
            }
        }
    };
    evidence.intent = json!({"intent": intent});
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
    // Book the first request of every selected source in any window its source advertises, so
    // the question waits for room instead of losing sources to a refusal mid-fetch. Without room
    // in about one window, the question is refused as busy: only routing was spent.
    let demands = connectors::first_requests(
        sources
            .iter()
            .filter(|source| selected.contains(&source.id)),
    );
    match http.book_windows(&demands).await {
        Ok(None) => {}
        Ok(Some(wait)) => {
            evidence.failures.push(failure(
                "admission",
                None,
                format!(
                    "No room in a source's request window within the booking limit; retry in {} ms",
                    wait.as_millis()
                ),
            ));
            evidence.retry_after_ms = Some(wait.as_millis() as u64);
            evidence.load = http.load_summary();
            return persist(config, &evidence, &backend.usage(), "busy");
        }
        Err(error) => {
            evidence
                .failures
                .push(failure("host_state", None, error.to_string()));
        }
    }
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
    .buffer_unordered(selected.len().max(1));
    // Every selected connector starts at once; the recorder bounds requests per host, so a slow
    // host cannot delay the connectors of other hosts.
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
                    evidence.failures.push(failure("fetch_deadline", Some(source_id), format!("Connector did not finish within --fetch-deadline-secs {}; its documents were not admitted.{}", config.fetch_deadline_secs, if config.full_record { " Completed raw responses remain under raw/." } else { "" })));
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
    write_json(
        config.output_dir.join("retrieved.json"),
        &retrieved_index(&fetched),
    )?;
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
                            "Duplicate document ID {} was not scored (kept in omitted.json with --full-record)",
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
        evidence.failures.push(failure(
            "document_limit",
            Some(source_id),
            format!(
                "Omitted {count} fetched documents from scoring; see --max-documents{}",
                if config.full_record {
                    " and omitted.json"
                } else {
                    ""
                }
            ),
        ));
    }
    persist(config, &evidence, &backend.usage(), "running")?;
    // Exact duplicates are scored once. Jev sees the title and the text, so both are in the key.
    // The score, or the failure, fans back to every original ID with its own provenance, so counts,
    // labels, and run status do not change.
    let mut representatives: BTreeMap<(String, String, String), String> = BTreeMap::new();
    let mut duplicates: BTreeMap<String, Vec<Document>> = BTreeMap::new();
    let mut to_score = Vec::new();
    for document in evidence.documents.clone() {
        if document.url.is_empty() {
            to_score.push(document);
            continue;
        }
        // A tuple key, not a joined string: a separator can appear inside either field.
        let key = (
            document.url.clone(),
            document.title.clone(),
            text_digest(&document.text),
        );
        match representatives.get(&key) {
            Some(representative) => duplicates
                .entry(representative.clone())
                .or_default()
                .push(document),
            None => {
                representatives.insert(key, document.id.clone());
                to_score.push(document);
            }
        }
    }
    // Keep Jev budget and retry accounting inside JevClient. Do not cancel its paid requests externally.
    // Groups of documents go to the scorer together, so their chunks can share Jev calls.
    let groups: Vec<Vec<Document>> = to_score
        .chunks(config.jev_batch)
        .map(<[Document]>::to_vec)
        .collect();
    let scoring = stream::iter(groups)
        .map(|group| async move {
            let results = backend.score_documents(question, &group).await;
            group.into_iter().zip(results).collect::<Vec<_>>()
        })
        .buffer_unordered(config.jev_concurrency)
        .flat_map(stream::iter);
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
                for copy in duplicates.remove(&document.id).unwrap_or_default() {
                    let mut copied = score.clone();
                    copied.document_id = copy.id.clone();
                    copied.reason = format!(
                        "Score copied from {} (same URL, title, and text). {}",
                        document.id, score.reason
                    );
                    classify(&mut evidence, config, copy, copied);
                }
                classify(&mut evidence, config, document, score);
            }
            Err(error) => {
                evidence.failures.push(failure(
                    "document_score",
                    Some(&document.source_id),
                    format!("{}: {error}", document.id),
                ));
                // Each copy gets its own failure row, so per-source failure counts match a run
                // that scored every copy separately.
                for copy in duplicates.remove(&document.id).unwrap_or_default() {
                    evidence.failures.push(failure(
                        "document_score",
                        Some(&copy.source_id),
                        format!(
                            "{}: not scored; the identical document {} failed: {error}",
                            copy.id, document.id
                        ),
                    ));
                    evidence.uncertain.push(copy);
                }
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
    if intent.asks_currentness() {
        let today = crate::rank::iso_date_days(&config.today).unwrap_or_default();
        propagate_url_dates(&mut evidence, today);
        if !config.fixture {
            backfill_page_dates(&http, &mut evidence, today).await;
            propagate_url_dates(&mut evidence, today);
        }
        assess_currentness(question, config, backend, &mut evidence, today).await;
        evidence
            .timings
            .insert("currentness", run_started.elapsed().as_millis() as u64);
    }
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
    evidence.load = http.load_summary();
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

fn text_digest(text: &str) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

/// Selected documents judged for currentness, highest relevance first. Lower-ranked documents keep
/// no judgment and take the middle position in that ranking list.
const CURRENTNESS_DOCUMENTS: usize = 80;
/// Selected pages whose HTML is read for page dates, and the time allowed for all of them.
const PAGE_DATE_FETCHES: usize = 24;
const PAGE_DATE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

/// A selected document without a date takes the newest date of any fetched document with the same
/// URL. Different sources index the same page; one of them may carry its date.
fn propagate_url_dates(evidence: &mut Evidence, today: i64) {
    let recency_date = |d: &Document| {
        crate::rank::document_date(d, today).filter(|x| crate::rank::drives_recency(x.kind))
    };
    let mut by_url: BTreeMap<String, crate::rank::DocDate> = BTreeMap::new();
    for document in evidence
        .selected
        .iter()
        .chain(&evidence.uncertain)
        .chain(&evidence.rejected)
    {
        if document.url.is_empty() {
            continue;
        }
        if let Some(date) = recency_date(document) {
            let newer = by_url
                .get(&document.url)
                .is_none_or(|known| date.days > known.days);
            if newer {
                by_url.insert(document.url.clone(), date);
            }
        }
    }
    for document in &mut evidence.selected {
        if recency_date(document).is_some() {
            continue;
        }
        if let Some(date) = by_url.get(&document.url) {
            document.provenance["url_date"] = json!({"date": date.date, "kind": date.kind});
        }
    }
}

/// Developer-docs and site pages are scored from their Markdown or index text, which carries no
/// date; the HTML page states it in machine-readable metadata. Read that metadata for selected
/// canonical pages that are still undated, within a small request and time budget.
async fn backfill_page_dates(http: &HttpRecorder, evidence: &mut Evidence, today: i64) {
    let targets: Vec<usize> = evidence
        .selected
        .iter()
        .enumerate()
        .filter(|(_, d)| {
            d.source_id.starts_with("algolia:")
                && connectors::algolia::is_canonical_page(&d.url)
                && !crate::rank::document_date(d, today)
                    .is_some_and(|x| crate::rank::drives_recency(x.kind))
        })
        .map(|(i, _)| i)
        .take(PAGE_DATE_FETCHES)
        .collect();
    let fetches = targets.iter().map(|&i| {
        let url = evidence.selected[i].url.clone();
        async move {
            let response = tokio::time::timeout(
                PAGE_DATE_DEADLINE,
                http.request(reqwest::Method::GET, &url, vec![], None),
            )
            .await
            .ok()?
            .ok()?;
            (response.status == 200)
                .then(|| crate::rank::html_page_dates(&String::from_utf8_lossy(&response.body)))
                .map(|dates| (i, dates))
        }
    });
    // Each fetch has its own time limit, so one slow page never discards the others.
    for (i, (modified, published)) in futures::future::join_all(fetches)
        .await
        .into_iter()
        .flatten()
    {
        if modified.is_some() || published.is_some() {
            evidence.selected[i].provenance["page_dates"] =
                json!({"modified": modified, "published": published});
        }
    }
}

/// Ask Jev, for each selected document, whether its best chunk likely still holds today given the
/// document's date. Relevance and selection are already final; this only informs the order.
/// Identical inputs (same title, chunk, and date) are asked once and the answer is copied.
async fn assess_currentness(
    question: &str,
    config: &RunConfig,
    backend: &dyn Backend,
    evidence: &mut Evidence,
    today: i64,
) {
    let scores: BTreeMap<String, (f64, [usize; 2])> = evidence
        .scores
        .iter()
        .map(|s| (s.document_id.clone(), (s.usable_top2_mean, s.best_chunk)))
        .collect();
    let mut targets: Vec<&Document> = evidence
        .selected
        .iter()
        .filter(|d| scores.contains_key(&d.id))
        .collect();
    targets.sort_by(|a, b| {
        scores[&b.id]
            .0
            .total_cmp(&scores[&a.id].0)
            .then(a.id.cmp(&b.id))
    });
    // Group identical inputs, keeping the order of their first member.
    type Group<'a> = (&'a Document, Option<crate::rank::DocDate>, Vec<String>);
    let mut groups: Vec<Group> = Vec::new();
    let mut by_input: BTreeMap<(String, String, Option<String>), usize> = BTreeMap::new();
    for document in targets {
        let [start, end] = scores[&document.id].1;
        let chunk = document.text.get(start..end).unwrap_or(&document.text);
        let date = crate::rank::document_date(document, today);
        let key = (
            document.title.clone(),
            text_digest(chunk),
            date.as_ref().map(|d| format!("{} {}", d.date, d.kind)),
        );
        match by_input.get(&key) {
            Some(&g) => groups[g].2.push(document.id.clone()),
            None if groups.len() < CURRENTNESS_DOCUMENTS => {
                by_input.insert(key, groups.len());
                groups.push((document, date, vec![document.id.clone()]));
            }
            None => {}
        }
    }
    let today_text = config.today.as_str();
    let results: Vec<(Vec<String>, Result<f64>)> = stream::iter(groups)
        .map(|(document, date, ids)| {
            let chunk = scores[&document.id].1;
            async move {
                let date = date.as_ref().map(|d| (d.date.as_str(), d.kind));
                let result = backend
                    .assess_currentness(question, document, chunk, date, today_text)
                    .await;
                (ids, result)
            }
        })
        .buffer_unordered(config.jev_concurrency)
        .collect()
        .await;
    let mut judged: BTreeMap<String, f64> = BTreeMap::new();
    for (ids, result) in results {
        match result {
            Ok(value) if value.is_finite() && (0.0..=1.0).contains(&value) => {
                for id in ids {
                    judged.insert(id, value);
                }
            }
            Ok(_) => evidence.failures.push(failure(
                "currentness",
                None,
                format!(
                    "{}: Jev returned an invalid currentness probability",
                    ids[0]
                ),
            )),
            Err(error) => {
                evidence
                    .failures
                    .push(failure("currentness", None, format!("{}: {error}", ids[0])))
            }
        }
    }
    for score in &mut evidence.scores {
        score.still_current = judged.get(&score.document_id).copied();
    }
}

fn classify(evidence: &mut Evidence, config: &RunConfig, document: Document, score: DocumentScore) {
    if score.probability >= config.document_threshold {
        evidence.selected.push(document);
    } else if score.probability >= config.uncertain_threshold {
        evidence.uncertain.push(document);
    } else {
        evidence.rejected.push(document);
    }
    evidence.scores.push(score);
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
    }
    impl Mock {
        async fn score_one(&self, _: &str, document: &Document) -> Result<DocumentScore> {
            self.scored.fetch_add(1, Ordering::SeqCst);
            if self.fail_score {
                bail!("actual scoring failure");
            }
            Ok(DocumentScore {
                document_id: document.id.clone(),
                probability: 0.8,
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
            let scores: Vec<DocumentScore> = serde_json::from_slice(
                &std::fs::read(outcome.directory.join("scores.json")).unwrap(),
            )
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
        let failures: Vec<Failure> = serde_json::from_slice(
            &std::fs::read(outcome.directory.join("failures.json")).unwrap(),
        )
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
