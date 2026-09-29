use crate::{connectors, http::HttpRecorder, jev::JevClient, types::*};
use anyhow::{bail, ensure, Context, Result};
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
pub(crate) struct Evidence {
    routes: Vec<serde_json::Value>,
    source_decisions: Vec<serde_json::Value>,
    documents: Vec<Document>,
    scores: Vec<DocumentScore>,
    selected: Vec<Document>,
    rejected: Vec<Document>,
    uncertain: Vec<Document>,
    omitted: Vec<Document>,
    /// Fetched and admitted but not yet scored (`--score-depth`); `more` can score them.
    deferred: Vec<Document>,
    failures: Vec<Failure>,
    /// The question time intent. See `rank`.
    intent: serde_json::Value,
    /// Source load counters from the HTTP recorder. See `HttpRecorder::load_summary`.
    load: serde_json::Value,
    /// Set when the question found no room in a source's request window in time.
    #[serde(skip)]
    retry_after_ms: Option<u64>,
    /// This call's original page reads, for `load`.
    #[serde(skip)]
    original_reads: serde_json::Value,
    /// The session's usage before this call, so checkpoints save cumulative usage.
    #[serde(skip)]
    prior_usage: Usage,
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
    /// The client for original page reads. See `HttpRecorder::public_reader`.
    fn original_reader(&self, http: &HttpRecorder) -> Result<HttpRecorder> {
        http.public_reader()
    }
    /// The free network check that runs before the first reservation. See
    /// `JevClient::check_network`.
    async fn check_network(&self) -> Result<()> {
        Ok(())
    }
    /// Providers that the network check took out of this run. See
    /// `JevClient::skipped_providers`.
    fn skipped_providers(&self) -> BTreeMap<String, String> {
        BTreeMap::new()
    }
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
    async fn check_network(&self) -> Result<()> {
        self.jev.check_network().await
    }
    fn skipped_providers(&self) -> BTreeMap<String, String> {
        self.jev.skipped_providers()
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
        ("fetch-threshold", config.fetch_threshold),
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
    // Absolute, so the session id works from any directory.
    config.output_dir = config.output_dir.canonicalize()?;
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
    write_json(root.join("deferred.json"), &evidence.deferred)?;
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

/// Files a session needs for later calls (`more`, `check`, `report`).
pub(crate) const SESSION_FILES: &[&str] = &[
    "question.json",
    "routes.json",
    "source-scope.json",
    "source-decisions.json",
    "documents.json",
    "deferred.json",
    "scores.json",
    "classification.json",
    "omitted.json",
    "failures.json",
    "intent.json",
    "usage.json",
    "load.json",
    "retrieved.json",
    "session.json",
    "bundle.md",
];

/// The default light record: keep the report, the text files its text_path fields name, the
/// manifest, and the session state; remove raw bodies, Jev traces, and intermediate files. Run
/// after the report is built.
pub fn keep_session_record(root: &Path) -> Result<()> {
    anyhow::ensure!(
        root.join("search.json").is_file() && root.join("manifest.json").is_file(),
        "The report must exist before the run folder is reduced to it"
    );
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if matches!(
            name.as_ref(),
            "manifest.json" | "search.json" | "search-documents" | "checks" | "session.lock"
        )
            || SESSION_FILES.contains(&name.as_ref())
            // Replay variants: search-NAME.json and search-documents-NAME.
            || name.starts_with("search-")
        {
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
                cause: None,
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
        cause: None,
    }
}

/// The report for a Jev judgment of `subject` that has no answer. A refusal after Jev stopped
/// paid work gets its own stage: the item was not assessed, which is not a failed `stage`. Any
/// other failure keeps `stage` and carries its cause class.
fn jev_failure(stage: &str, source: Option<&str>, subject: &str, error: &anyhow::Error) -> Failure {
    let subject = if subject.is_empty() {
        String::new()
    } else {
        format!("{subject}: ")
    };
    if crate::jev::not_assessed_after_stop(error) {
        return failure(
            NOT_ASSESSED_AFTER_STOP,
            source,
            format!("{subject}{stage} not assessed: {error}"),
        );
    }
    Failure {
        cause: crate::jev::failure_cause(error).map(str::to_owned),
        ..failure(stage, source, format!("{subject}{error}"))
    }
}

/// The report stage of an item that Jev did not assess because it had stopped paid work.
pub(crate) const NOT_ASSESSED_AFTER_STOP: &str = "not_assessed_after_stop";

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
    // No Jev call reserves anything before this free check passes. Without Jev, nothing can be
    // routed, so a failed check ends the run here.
    if let Err(error) = backend.check_network().await {
        evidence
            .failures
            .push(jev_failure("network_check", None, "", &error));
        evidence.load = json!({"jev_providers_skipped": backend.skipped_providers()});
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
                .push(jev_failure("intent", None, "", &error));
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
                    .push(jev_failure("route", None, &format!("Pass {pass}"), &error));
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
        let now = selected.contains(&source.id)
            && maxima
                .get(&source.id)
                .is_some_and(|p| *p >= config.fetch_threshold);
        // `fetched` turns true once the fetch ran, so a call refused before it keeps the source
        // as a pool.
        evidence.source_decisions.push(json!({"source_id":source.id,"max_probability":maxima.get(&source.id),"selected":selected.contains(&source.id),"fetched":false,"reason":if now {"selected by union across passes"} else if selected.contains(&source.id) {"routed below the fetch threshold; kept in the session as a pool"} else if valid_passes == 0 {"not retrieved because all route passes failed"} else {"below source threshold in all valid passes"}}));
    }
    if valid_passes == 0 {
        return persist(config, &evidence, &backend.usage(), "failed");
    }
    evidence
        .timings
        .insert("route", run_started.elapsed().as_millis() as u64);
    // Sources routed at or above the fetch threshold are fetched now; the others stay in the
    // session as pools that `more` can spend.
    let fetch_now: Vec<&Source> = sources
        .iter()
        .filter(|source| {
            selected.contains(&source.id)
                && maxima
                    .get(&source.id)
                    .is_some_and(|p| *p >= config.fetch_threshold)
        })
        .collect();
    // Hold a fetch slot on every capped host this question fetches from, so a busy host queues
    // questions instead of starting fetches that its sources cannot finish in time.
    let Some(hold) = http
        .hold_hosts(
            &connectors::host_caps(fetch_now.iter().copied(), config),
            HOST_SLOT_WAIT,
        )
        .await?
    else {
        evidence.failures.push(failure(
            "admission",
            None,
            "No fetch slot on a capped source host within the wait limit",
        ));
        evidence.retry_after_ms = Some(HOST_SLOT_RETRY_MS);
        evidence.load = http.load_summary();
        return persist(config, &evidence, &backend.usage(), "busy");
    };
    // Book the first request of every source fetched now in any window its source advertises, so
    // the question waits for room instead of losing sources to a refusal mid-fetch. Without room
    // in about one window, the question is refused as busy: only routing was spent.
    match http
        .book_windows(&connectors::first_requests(fetch_now.iter().copied()))
        .await
    {
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
    let fetched =
        fetch_sources(question, config, &http, backend, &fetch_now, &mut evidence).await?;
    drop(hold);
    mark_fetched(&mut evidence, &fetch_now);
    evidence
        .timings
        .insert("fetch", run_started.elapsed().as_millis() as u64);
    let admitted = admit(config, &mut evidence, fetched);
    let originals = plan_originals(config, &evidence, &admitted, &maxima, sources)?;
    persist(config, &evidence, &backend.usage(), "running")?;
    let (scored, read) = tokio::join!(
        score_stage(question, config, backend, &mut evidence, admitted, &maxima),
        read_originals(backend, &http, &originals.reads)
    );
    scored?;
    add_originals(question, config, backend, &mut evidence, originals, read).await?;
    finish(
        question,
        config,
        &http,
        backend,
        evidence,
        &intent,
        &Usage::default(),
        run_started,
    )
    .await
}

/// How long a question waits for fetch slots on capped source hosts before it reports busy.
const HOST_SLOT_WAIT: std::time::Duration = std::time::Duration::from_secs(65);
/// The retry hint for a question refused for want of a host fetch slot.
const HOST_SLOT_RETRY_MS: u64 = 10_000;

/// Fetch the chosen sources in parallel within the fetch deadline. A connector still running at
/// the deadline is recorded and dropped.
pub(crate) async fn fetch_sources(
    question: &str,
    config: &RunConfig,
    http: &HttpRecorder,
    backend: &dyn Backend,
    chosen: &[&Source],
    evidence: &mut Evidence,
) -> Result<Vec<Document>> {
    let selected: BTreeSet<String> = chosen.iter().map(|s| s.id.clone()).collect();
    let fetch_deadline =
        tokio::time::Instant::now() + std::time::Duration::from_secs(config.fetch_deadline_secs);
    let jobs = stream::iter(chosen.iter().copied())
        .map(|source| {
            let mut source_config = config.clone();
            source_config.max_documents = config.max_documents.min(config.per_source_documents);
            let ctx = FetchContext {
                http: http.clone(),
                config: source_config,
                deadline: Some(fetch_deadline),
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
    record_retrieved(config, &fetched)?;
    Ok(fetched)
}

/// Add this call's fetched documents to retrieved.json. Later session calls add to the index, so
/// every call's retrieval stays on record. The file exists from the first call on; an unreadable
/// one is an error, never a reset.
fn record_retrieved(config: &RunConfig, fetched: &[Document]) -> Result<()> {
    let mut index: Vec<serde_json::Value> = read_json(&config.output_dir, "retrieved.json")?;
    let call = crate::session::current_call(&config.output_dir)?;
    index.extend(retrieved_index(fetched).into_iter().map(|mut r| {
        r["call"] = json!(call);
        r
    }));
    write_json(config.output_dir.join("retrieved.json"), &index)
}

/// Rows of sources routed at or above this are read at their original URL in the call that
/// fetches them.
const ORIGINAL_ROUTE: f64 = 0.6;
/// Original pages read at once, and the time allowed for all of a call's reads.
const ORIGINAL_CONCURRENCY: usize = 2;
const ORIGINAL_PHASE: std::time::Duration = std::time::Duration::from_secs(8);

/// A call's original page reads: the reserved requests, in order, and what planning decided.
#[derive(Default)]
struct OriginalPlan {
    reads: Vec<connectors::original::Candidate>,
    eligible: usize,
    reused: usize,
    capped: usize,
}

/// Choose this call's original page reads: eligible rows of sources routed at `ORIGINAL_ROUTE` or
/// above, by the source's routing probability and then row order, one per normalized URL. A URL
/// whose complete body the session holds is reused without a request. Requests are reserved in
/// the session within its caps; the rest are counted as capped.
fn plan_originals(
    config: &RunConfig,
    evidence: &Evidence,
    rows: &[Document],
    routes: &BTreeMap<String, f64>,
    sources: &[Source],
) -> Result<OriginalPlan> {
    if config.fixture {
        return Ok(OriginalPlan::default());
    }
    let route = |d: &Document| routes.get(&d.source_id).copied().unwrap_or(0.0);
    let mut ordered: Vec<&Document> = rows.iter().filter(|d| route(d) >= ORIGINAL_ROUTE).collect();
    // A stable sort keeps each source's row order.
    ordered.sort_by(|a, b| route(b).total_cmp(&route(a)));
    let plans = connectors::original::plan(
        ordered,
        evidence.documents.iter().chain(&evidence.deferred),
        sources,
    );
    let mut plan = OriginalPlan {
        eligible: plans.len(),
        ..Default::default()
    };
    for entry in plans {
        match entry {
            connectors::original::Plan::Read(candidate) => plan.reads.push(candidate),
            connectors::original::Plan::Reused { .. } => plan.reused += 1,
        }
    }
    let wanted = plan.reads.len().min(config.original_reads);
    let granted = if wanted == 0 {
        0
    } else {
        crate::session::reserve_original_reads(&config.output_dir, wanted)?
    };
    plan.capped = plan.reads.len() - granted;
    plan.reads.truncate(granted);
    Ok(plan)
}

/// What a call's reads returned: finished outcomes, the reads cut at the phase deadline, and how
/// many requests started.
type OriginalResults = (
    Vec<(
        connectors::original::Candidate,
        connectors::original::Outcome,
    )>,
    Vec<connectors::original::Candidate>,
    usize,
);

/// Read the planned pages, `ORIGINAL_CONCURRENCY` at a time, until `ORIGINAL_PHASE` ends. A read
/// still running then is dropped; the recorder keeps its receipt.
async fn read_originals(
    backend: &dyn Backend,
    http: &HttpRecorder,
    reads: &[connectors::original::Candidate],
) -> OriginalResults {
    use connectors::original::Outcome;
    if reads.is_empty() {
        return (Vec::new(), Vec::new(), 0);
    }
    let reader = match backend.original_reader(http) {
        Ok(reader) => reader,
        Err(error) => {
            let failed = reads
                .iter()
                .map(|c| {
                    (
                        c.clone(),
                        Outcome::Failed(format!("No public reader: {error}")),
                    )
                })
                .collect();
            return (failed, Vec::new(), 0);
        }
    };
    let started = std::sync::atomic::AtomicUsize::new(0);
    let mut jobs = Box::pin(
        stream::iter(reads.iter().enumerate())
            .map(|(i, candidate)| {
                let (reader, started) = (&reader, &started);
                async move {
                    started.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    (
                        i,
                        connectors::original::read_original(candidate, reader).await,
                    )
                }
            })
            .buffer_unordered(ORIGINAL_CONCURRENCY),
    );
    let deadline = tokio::time::Instant::now() + ORIGINAL_PHASE;
    let mut done = BTreeMap::new();
    while let Ok(Some((i, outcome))) = tokio::time::timeout_at(deadline, jobs.next()).await {
        done.insert(i, outcome);
    }
    drop(jobs);
    let mut finished = Vec::new();
    let mut cut = Vec::new();
    for (i, candidate) in reads.iter().enumerate() {
        match done.remove(&i) {
            Some(outcome) => finished.push((candidate.clone(), outcome)),
            None => cut.push(candidate.clone()),
        }
    }
    (
        finished,
        cut,
        started.load(std::sync::atomic::Ordering::Relaxed),
    )
}

/// Settle the session's reservation, report failed reads, count the reads for `load`, and append
/// each read page as a new document scored in its own batch. Existing documents keep their
/// positions, so their text paths do not change.
async fn add_originals(
    question: &str,
    config: &RunConfig,
    backend: &dyn Backend,
    evidence: &mut Evidence,
    plan: OriginalPlan,
    (finished, cut, started): OriginalResults,
) -> Result<()> {
    use connectors::original::Outcome;
    if config.fixture {
        return Ok(());
    }
    crate::session::release_original_reads(
        &config.output_dir,
        plan.reads.len().saturating_sub(started),
    )?;
    let (mut refused, mut failed) = (0, 0);
    let mut pages = Vec::new();
    for (candidate, outcome) in finished {
        let message = match outcome {
            Outcome::Read(document) => {
                pages.push(document);
                continue;
            }
            Outcome::Refused(message) => {
                refused += 1;
                message
            }
            Outcome::Failed(message) => {
                failed += 1;
                message
            }
        };
        evidence.failures.push(failure(
            "original_read",
            Some(&candidate.parent_source_id),
            format!(
                "The original page {} of {} was not read: {message}",
                candidate.url, candidate.parent_id
            ),
        ));
    }
    for candidate in &cut {
        failed += 1;
        evidence.failures.push(failure(
            "original_read",
            Some(&candidate.parent_source_id),
            format!(
                "The original page {} of {} was cut at the {}-second read deadline",
                candidate.url,
                candidate.parent_id,
                ORIGINAL_PHASE.as_secs()
            ),
        ));
    }
    record_retrieved(config, &pages)?;
    let held: BTreeSet<String> = evidence
        .documents
        .iter()
        .chain(&evidence.deferred)
        .map(|d| d.id.clone())
        .collect();
    let pages: Vec<Document> = pages
        .into_iter()
        .map(|mut d| {
            d.id = format!("{}::{}", d.source_id, d.id);
            d
        })
        .filter(|d| !held.contains(&d.id))
        .collect();
    evidence.original_reads = json!({
        "eligible": plan.eligible, "reused": plan.reused, "capped": plan.capped,
        "attempted": started, "used": pages.len(), "refused": refused, "failed": failed,
        "session_charged": crate::session::original_reads(&config.output_dir),
    });
    if !pages.is_empty() {
        score_batch(question, config, backend, evidence, pages).await?;
    }
    Ok(())
}

/// Namespace fetched documents by source and admit them round-robin across sources, each source
/// in upstream order, up to the session's document limit. Returns them in admission order.
pub(crate) fn admit(
    config: &RunConfig,
    evidence: &mut Evidence,
    fetched: Vec<Document>,
) -> Vec<Document> {
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
    let mut seen: BTreeSet<String> = evidence
        .documents
        .iter()
        .chain(&evidence.deferred)
        .map(|d| d.id.clone())
        .collect();
    let capacity = config.max_documents.saturating_sub(seen.len());
    let mut admitted = Vec::new();
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
                } else if admitted.len() < capacity {
                    admitted.push(document);
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
    admitted
}

/// Score documents with Jev and classify them. Exact duplicates, within the batch or of a document
/// the session already scored, are scored once and the score, or the failure, is copied.
pub(crate) async fn score_batch(
    question: &str,
    config: &RunConfig,
    backend: &dyn Backend,
    evidence: &mut Evidence,
    batch: Vec<Document>,
) -> Result<()> {
    // Exact duplicates are scored once. Jev sees the title and the text, so both are in the key.
    // The score, or the failure, fans back to every original ID with its own provenance, so counts,
    // labels, and run status do not change.
    // A tuple key, not a joined string: a separator can appear inside either field.
    let key = |document: &Document| {
        (
            document.url.clone(),
            document.title.clone(),
            text_digest(&document.text),
        )
    };
    // Documents this session already scored, so a later call reuses their scores.
    let scored: BTreeMap<&str, &DocumentScore> = evidence
        .scores
        .iter()
        .map(|s| (s.document_id.as_str(), s))
        .collect();
    let known: BTreeMap<(String, String, String), DocumentScore> = evidence
        .documents
        .iter()
        .filter(|d| !d.url.is_empty())
        .filter_map(|d| scored.get(d.id.as_str()).map(|s| (key(d), (*s).clone())))
        .collect();
    // A document already stored (left unscored by a stopped call) keeps its position.
    let stored: BTreeSet<String> = evidence.documents.iter().map(|d| d.id.clone()).collect();
    evidence
        .documents
        .extend(batch.iter().filter(|d| !stored.contains(&d.id)).cloned());
    let mut representatives: BTreeMap<(String, String, String), String> = BTreeMap::new();
    let mut duplicates: BTreeMap<String, Vec<Document>> = BTreeMap::new();
    let mut to_score = Vec::new();
    for document in batch {
        if document.url.is_empty() {
            to_score.push(document);
            continue;
        }
        let key = key(&document);
        if let Some(score) = known.get(&key) {
            let mut copied = score.clone();
            copied.reason = format!(
                "Score copied from {} (same URL, title, and text). {}",
                score.document_id, score.reason
            );
            copied.document_id = document.id.clone();
            // Currentness depends on the document's own date, which can differ.
            copied.still_current = None;
            classify(evidence, config, document, copied);
            continue;
        }
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
                    classify(evidence, config, copy, copied);
                }
                classify(evidence, config, document, score);
            }
            Err(error) => {
                evidence.failures.push(jev_failure(
                    "document_score",
                    Some(&document.source_id),
                    &document.id,
                    &error,
                ));
                // Each copy gets its own failure row, so per-source failure counts match a run
                // that scored every copy separately.
                for copy in duplicates.remove(&document.id).unwrap_or_default() {
                    evidence.failures.push(jev_failure(
                        "document_score",
                        Some(&copy.source_id),
                        &format!(
                            "{}: not scored; the identical document {} failed",
                            copy.id, document.id
                        ),
                        &error,
                    ));
                    evidence.uncertain.push(copy);
                }
                evidence.uncertain.push(document);
            }
        }
        if checkpoint.elapsed() >= CHECKPOINT_INTERVAL {
            persist(
                config,
                evidence,
                &add_usage(&evidence.prior_usage, &backend.usage()),
                "running",
            )?;
            checkpoint = std::time::Instant::now();
        }
    }
    Ok(())
}

/// A source whose routing reached this is promising enough to score beyond `--score-depth`.
const EXTEND_ROUTE: f64 = 0.6;

/// Score admitted documents. With `--score-depth`, score the first documents of each source,
/// then the rest of a source only where it routed at `EXTEND_ROUTE` or above or one of its scored
/// documents reached `--uncertain-threshold`; other documents stay in the session unscored.
pub(crate) async fn score_stage(
    question: &str,
    config: &RunConfig,
    backend: &dyn Backend,
    evidence: &mut Evidence,
    admitted: Vec<Document>,
    routes: &BTreeMap<String, f64>,
) -> Result<()> {
    if config.score_depth == 0 {
        return score_batch(question, config, backend, evidence, admitted).await;
    }
    let mut first = Vec::new();
    let mut tails: BTreeMap<String, Vec<Document>> = BTreeMap::new();
    let mut taken: BTreeMap<String, usize> = BTreeMap::new();
    for document in admitted {
        let count = taken.entry(document.source_id.clone()).or_default();
        if *count < config.score_depth {
            *count += 1;
            first.push(document);
        } else {
            tails
                .entry(document.source_id.clone())
                .or_default()
                .push(document);
        }
    }
    score_batch(question, config, backend, evidence, first).await?;
    let source_of: BTreeMap<&str, &str> = evidence
        .documents
        .iter()
        .map(|d| (d.id.as_str(), d.source_id.as_str()))
        .collect();
    let promising: BTreeSet<String> = evidence
        .scores
        .iter()
        .filter(|s| s.probability >= config.uncertain_threshold)
        .filter_map(|s| {
            source_of
                .get(s.document_id.as_str())
                .map(|s| (*s).to_owned())
        })
        .collect();
    let mut second = Vec::new();
    for (source, tail) in tails {
        if promising.contains(&source) || routes.get(&source).is_some_and(|p| *p >= EXTEND_ROUTE) {
            second.extend(tail);
        } else {
            evidence.deferred.extend(tail);
        }
    }
    if !second.is_empty() {
        score_batch(question, config, backend, evidence, second).await?;
    }
    Ok(())
}

/// Judge currentness where the intent asks for it, then save the run and its manifest. `prior` is
/// the session's usage before this call.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn finish(
    question: &str,
    config: &RunConfig,
    http: &HttpRecorder,
    backend: &dyn Backend,
    mut evidence: Evidence,
    intent: &crate::rank::Intent,
    prior: &Usage,
    run_started: std::time::Instant,
) -> Result<RunOutcome> {
    evidence
        .timings
        .insert("score", run_started.elapsed().as_millis() as u64);
    if intent.asks_currentness() {
        let today = crate::rank::iso_date_days(&config.today).unwrap_or_default();
        propagate_url_dates(&mut evidence, today);
        if !config.fixture {
            backfill_page_dates(http, &mut evidence, today).await;
            propagate_url_dates(&mut evidence, today);
        }
        assess_currentness(question, config, backend, &mut evidence, today).await;
        // Dates found for selected documents belong to the stored documents too.
        let dated: BTreeMap<String, serde_json::Value> = evidence
            .selected
            .iter()
            .map(|d| (d.id.clone(), d.provenance.clone()))
            .collect();
        for document in &mut evidence.documents {
            if let Some(provenance) = dated.get(&document.id) {
                document.provenance = provenance.clone();
            }
        }
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
    evidence.load["jev_providers_skipped"] = json!(backend.skipped_providers());
    if !evidence.original_reads.is_null() {
        evidence.load["original_reads"] = evidence.original_reads.clone();
    }
    let status = if evidence.failures.is_empty() {
        "complete"
    } else {
        "partial"
    };
    let usage = add_usage(prior, &backend.usage());
    let outcome = persist(config, &evidence, &usage, status)
        .context("Could not finalize the run evidence")?;
    // Record the export cost without rewriting every artifact again.
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(config.output_dir.join("manifest.json"))?)?;
    manifest["phase_ms"]["finalize"] = json!(run_started.elapsed().as_millis() as u64);
    write_json(config.output_dir.join("manifest.json"), &manifest)?;
    Ok(outcome)
}

fn read_json<T: serde::de::DeserializeOwned>(root: &Path, name: &str) -> Result<T> {
    serde_json::from_slice(&std::fs::read(root.join(name))?)
        .with_context(|| format!("{name} in the session is unreadable"))
}

/// The session state saved in a run folder.
pub(crate) fn load_evidence(root: &Path, config: &RunConfig) -> Result<Evidence> {
    let documents: Vec<Document> = read_json(root, "documents.json")?;
    let classification: serde_json::Value = read_json(root, "classification.json")?;
    let by_id: BTreeMap<&str, &Document> = documents.iter().map(|d| (d.id.as_str(), d)).collect();
    let list = |status: &str| -> Result<Vec<Document>> {
        classification[status]
            .as_array()
            .with_context(|| format!("classification.json lacks {status}"))?
            .iter()
            .map(|id| {
                let id = id.as_str().context("Classification IDs must be strings")?;
                by_id
                    .get(id)
                    .map(|d| (*d).clone())
                    .with_context(|| format!("documents.json lacks classified document {id}"))
            })
            .collect()
    };
    // A call that stopped while scoring leaves documents that are neither scored nor classified.
    // They keep their positions (and so their text paths) and are scored by a later call.
    let scores: Vec<DocumentScore> = read_json(root, "scores.json")?;
    let scored: BTreeSet<&str> = scores.iter().map(|s| s.document_id.as_str()).collect();
    let classified: BTreeSet<&str> = ["selected", "uncertain", "rejected"]
        .iter()
        .flat_map(|status| classification[*status].as_array().into_iter().flatten())
        .filter_map(serde_json::Value::as_str)
        .collect();
    // A stopped call can also leave a scored or classified document in the unscored pool.
    let mut deferred: Vec<Document> = read_json::<Vec<Document>>(root, "deferred.json")?
        .into_iter()
        .filter(|d| !scored.contains(d.id.as_str()) && !classified.contains(d.id.as_str()))
        .collect();
    let pending: BTreeSet<String> = deferred.iter().map(|d| d.id.clone()).collect();
    deferred.extend(
        documents
            .iter()
            .filter(|d| {
                !scored.contains(d.id.as_str())
                    && !classified.contains(d.id.as_str())
                    && !pending.contains(&d.id)
            })
            .cloned(),
    );
    let mut evidence = Evidence {
        selected: list("selected")?,
        uncertain: list("uncertain")?,
        rejected: list("rejected")?,
        routes: read_json(root, "routes.json")?,
        source_decisions: read_json(root, "source-decisions.json")?,
        omitted: read_json(root, "omitted.json")?,
        failures: read_json(root, "failures.json")?,
        intent: read_json(root, "intent.json")?,
        scores: Vec::new(),
        deferred,
        documents,
        ..Evidence::default()
    };
    // A document scored just before a call stopped, but not yet classified, is classified now.
    let by_id: BTreeMap<String, Document> = evidence
        .documents
        .iter()
        .map(|d| (d.id.clone(), d.clone()))
        .collect();
    for score in scores {
        match by_id.get(&score.document_id) {
            Some(document) if !classified.contains(score.document_id.as_str()) => {
                classify(&mut evidence, config, document.clone(), score)
            }
            _ => evidence.scores.push(score),
        }
    }
    Ok(evidence)
}

/// Save the documents, scores, classification, and unscored pool of a session.
fn save_session_state(root: &Path, evidence: &Evidence) -> Result<()> {
    let ids = |documents: &[Document]| documents.iter().map(|d| d.id.clone()).collect::<Vec<_>>();
    write_json(root.join("documents.json"), &evidence.documents)?;
    write_json(root.join("scores.json"), &evidence.scores)?;
    write_json(
        root.join("classification.json"),
        &json!({"selected":ids(&evidence.selected),"uncertain":ids(&evidence.uncertain),"rejected":ids(&evidence.rejected)}),
    )?;
    write_json(root.join("deferred.json"), &evidence.deferred)
}

/// A session's cumulative Jev spending is capped at this multiple of `--budget-usd`.
pub const SESSION_BUDGET_MULTIPLE: f64 = 3.0;

/// What a session has not spent yet: unscored documents per source, and sources routed at or
/// above the source threshold but not fetched.
pub(crate) fn open_pools(evidence: &Evidence) -> (BTreeMap<String, usize>, Vec<String>) {
    let mut tails: BTreeMap<String, usize> = BTreeMap::new();
    for document in &evidence.deferred {
        *tails.entry(document.source_id.clone()).or_default() += 1;
    }
    let unfetched = evidence
        .source_decisions
        .iter()
        .filter(|d| d["selected"] == true && d["fetched"] == false)
        .filter_map(|d| d["source_id"].as_str().map(str::to_owned))
        .collect();
    (tails, unfetched)
}

/// Spend a session's pools: score the unscored documents of the named sources and fetch the named
/// sources that were not fetched, then re-rank the session. `None` spends every pool. Settings
/// come from the session; the budget and host folder come from this call.
pub async fn continue_session(
    root: &Path,
    pools: Option<&[String]>,
    current: &RunConfig,
) -> Result<RunOutcome> {
    let root = root
        .canonicalize()
        .context("The session folder does not exist")?;
    let record: serde_json::Value = read_json(&root, "question.json")?;
    let question = record["question"]
        .as_str()
        .context("question.json lacks the question")?
        .to_owned();
    let mut config: RunConfig = serde_json::from_value(record["config"].clone())
        .context("question.json has unreadable settings")?;
    config.output_dir = root.clone();
    config.host_dir = current.host_dir.clone();
    config.source_slots = current.source_slots.clone();
    config.original_reads = current.original_reads;
    config.source_hedge_ms = current.source_hedge_ms;
    let mut evidence = load_evidence(&root, &config)?;
    // Save what loading repaired after a stopped call, before anything can end this call early.
    save_session_state(&root, &evidence)?;
    // Charge a call that stopped earlier before this call's allowance is set.
    crate::session::settle_stopped_call(&root)?;
    let prior: Usage = read_json(&root, "usage.json")?;
    evidence.prior_usage = prior.clone();
    let cap = crate::session::budget_cap(&root)?;
    let remaining = cap - prior.cost_usd;
    ensure!(
        config.fixture || remaining > 0.001,
        "The session has spent its budget of ${cap:.3}"
    );
    config.budget_usd = current.budget_usd.min(remaining);
    let (tails, unfetched) = open_pools(&evidence);
    let requested: BTreeSet<String> = match pools {
        None => tails
            .keys()
            .cloned()
            .chain(unfetched.iter().cloned())
            .collect(),
        Some(ids) => {
            for id in ids {
                ensure!(
                    tails.contains_key(id) || unfetched.contains(id),
                    "{id} is not an open pool of this session; open pools: {}",
                    tails
                        .keys()
                        .chain(unfetched.iter())
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
            ids.iter().cloned().collect()
        }
    };
    ensure!(!requested.is_empty(), "The session has no open pools");
    // Mark this call as spending up to its budget until it saves its usage.
    crate::session::begin_call(&root, config.budget_usd)?;
    let governor = match &config.host_dir {
        Some(dir) => crate::governor::Governor::at(dir)?,
        None => crate::governor::Governor::local(),
    };
    let http =
        HttpRecorder::resume(&root, &config)?.with_source_gates(std::sync::Arc::new(governor));
    let jev = JevClient::new(&config, &http.with_concurrency(config.jev_concurrency))?;
    let backend = LiveBackend { jev };
    // Before any reservation: a failed free check leaves the session as it was.
    if let Err(error) = backend.check_network().await {
        crate::session::end_call(&root)?;
        return Err(error);
    }
    let run_started = std::time::Instant::now();
    let registry = connectors::sources();
    let chosen: Vec<&Source> = registry
        .iter()
        .filter(|source| unfetched.contains(&source.id) && requested.contains(&source.id))
        .collect();
    let caps = connectors::host_caps(chosen.iter().copied(), &config);
    let hold = http.hold_hosts(&caps, HOST_SLOT_WAIT).await?;
    let window_wait = match (&hold, chosen.is_empty()) {
        (None, _) => Some(std::time::Duration::from_millis(HOST_SLOT_RETRY_MS)),
        (Some(_), true) => None,
        (Some(_), false) => {
            http.book_windows(&connectors::first_requests(chosen.iter().copied()))
                .await?
        }
    };
    if let Some(wait) = window_wait {
        // Nothing in the session changes; the caller retries later.
        crate::session::end_call(&root)?;
        return Ok(RunOutcome {
            directory: root.clone(),
            status: "busy".into(),
            selected: evidence.selected.len(),
            rejected: evidence.rejected.len(),
            uncertain: evidence.uncertain.len(),
            failures: evidence.failures.len(),
            usage: prior,
            retry_after_ms: Some(wait.as_millis() as u64),
        });
    }
    let fetched =
        fetch_sources(&question, &config, &http, &backend, &chosen, &mut evidence).await?;
    drop(hold);
    mark_fetched(&mut evidence, &chosen);
    let admitted = admit(&config, &mut evidence, fetched);
    let (mut batch, kept): (Vec<Document>, Vec<Document>) = std::mem::take(&mut evidence.deferred)
        .into_iter()
        .partition(|d| requested.contains(&d.source_id));
    evidence.deferred = kept;
    batch.extend(admitted);
    let routes: BTreeMap<String, f64> = evidence
        .source_decisions
        .iter()
        .filter_map(|d| {
            Some((
                d["source_id"].as_str()?.to_owned(),
                d["max_probability"].as_f64()?,
            ))
        })
        .collect();
    let originals = plan_originals(&config, &evidence, &batch, &routes, &registry)?;
    let (scored, read) = tokio::join!(
        score_batch(&question, &config, &backend, &mut evidence, batch),
        read_originals(&backend, &http, &originals.reads)
    );
    scored?;
    add_originals(&question, &config, &backend, &mut evidence, originals, read).await?;
    let intent: crate::rank::Intent = if evidence.intent["intent"].is_null() {
        crate::rank::Intent {
            kind: "timeless".into(),
            confidence: 0.0,
            versioned: 0.0,
        }
    } else {
        serde_json::from_value(evidence.intent["intent"].clone())?
    };
    let outcome = finish(
        &question,
        &config,
        &http,
        &backend,
        evidence,
        &intent,
        &prior,
        run_started,
    )
    .await?;
    crate::session::end_call(&root)?;
    Ok(outcome)
}

/// Record that these sources were fetched.
fn mark_fetched(evidence: &mut Evidence, sources: &[&Source]) {
    for decision in &mut evidence.source_decisions {
        if decision["source_id"]
            .as_str()
            .is_some_and(|id| sources.iter().any(|s| s.id == id))
        {
            decision["fetched"] = json!(true);
        }
    }
}

/// The sum of two usage records.
pub(crate) fn add_usage(a: &Usage, b: &Usage) -> Usage {
    let mut provider_requests = a.provider_requests.clone();
    for (provider, count) in &b.provider_requests {
        *provider_requests.entry(provider.clone()).or_default() += count;
    }
    Usage {
        requests: a.requests + b.requests,
        input_tokens: a.input_tokens + b.input_tokens,
        output_tokens: a.output_tokens + b.output_tokens,
        cost_usd: a.cost_usd + b.cost_usd,
        hedged_requests: a.hedged_requests + b.hedged_requests,
        rate_limited_requests: a.rate_limited_requests + b.rate_limited_requests,
        provider_requests,
        provider_wait_ms: a.provider_wait_ms + b.provider_wait_ms,
    }
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
    // Documents judged in an earlier call of the session keep their judgment.
    let scores: BTreeMap<String, (f64, [usize; 2])> = evidence
        .scores
        .iter()
        .filter(|s| s.still_current.is_none())
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
            Err(error) => evidence
                .failures
                .push(jev_failure("currentness", None, &ids[0], &error)),
        }
    }
    for score in &mut evidence.scores {
        if let Some(value) = judged.get(&score.document_id) {
            score.still_current = Some(*value);
        }
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
        let mut classification: serde_json::Value =
            read_json(&root, "classification.json").unwrap();
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
