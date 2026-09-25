use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Source {
    pub id: String,
    pub name: String,
    pub description: String,
    pub family: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Document {
    pub id: String,
    pub source_id: String,
    pub title: String,
    pub url: String,
    pub text: String,
    pub provenance: Value,
    pub raw_artifacts: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Failure {
    pub stage: String,
    pub source_id: Option<String>,
    pub message: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FetchResult {
    pub documents: Vec<Document>,
    pub failures: Vec<Failure>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceScore {
    pub source_id: String,
    pub probability: f64,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DocumentScore {
    pub document_id: String,
    pub probability: f64,
    pub reason: String,
    /// Every Jev signal for the document. Empty for fixtures.
    pub signals: std::collections::BTreeMap<String, f64>,
    /// How `signals` was built. `independent_max_per_signal_across_chunks` means each value is the
    /// maximum of that signal over the document's chunks; different signals can come from different
    /// chunks, so the map is not one jointly supported evidence vector.
    pub signals_aggregation: String,
    /// Byte range of the chunk with the highest usable_evidence. `still_current` judges this chunk
    /// only, so its answer never mixes chunks.
    pub best_chunk: [usize; 2],
    /// Mean of the two highest chunk usable_evidence values (one chunk: that value). Long pages
    /// get more chunks and so more chances at a high maximum; this key reduces that advantage.
    pub usable_top2_mean: f64,
    /// Jev's judgment that the best chunk likely still holds today, given the document's date.
    /// Asked after selection, only for questions that depend on time.
    pub still_current: Option<f64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Usage {
    pub requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cost_usd: f64,
    /// Hedge requests sent for slow calls. They are included in `requests`.
    pub hedged_requests: u64,
    /// Requests a provider rejected with HTTP 429 or 529. They are not in `requests` and cost nothing.
    pub rate_limited_requests: u64,
    /// Settled requests per provider (`cloudflare`, `typesafe`, `openrouter`).
    pub provider_requests: std::collections::BTreeMap<String, u64>,
    /// Time calls waited because every usable provider was cooling or out of host send budget.
    pub provider_wait_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunConfig {
    pub fixture: bool,
    pub output_dir: PathBuf,
    pub budget_usd: f64,
    pub timeout_secs: u64,
    pub concurrency: usize,
    /// Concurrent Jev requests. Separate from source concurrency: source APIs have rate limits.
    pub jev_concurrency: usize,
    /// Send one hedge request for a Jev call still unanswered after this many milliseconds.
    /// Zero turns hedging off.
    pub jev_hedge_ms: u64,
    /// Send one hedge request, on a fresh connection, for a source GET still unanswered this many
    /// milliseconds after it was sent. Zero turns source hedging off.
    pub source_hedge_ms: u64,
    /// Document chunks packed into one Jev scoring call. 1 sends one chunk per call.
    pub jev_batch: usize,
    /// The reference date for currentness judgments, as YYYY-MM-DD in UTC.
    pub today: String,
    pub max_pages: usize,
    pub max_documents: usize,
    pub per_source_documents: usize,
    pub fetch_deadline_secs: u64,
    pub max_body_bytes: usize,
    pub route_passes: usize,
    pub source_threshold: f64,
    /// Sources routed at or above this are fetched in the first call; sources routed between
    /// `source_threshold` and this stay in the session as pools for `more`.
    pub fetch_threshold: f64,
    /// Score the first this many documents of each fetched source, then the rest only where the
    /// source routed high or a scored document reached `uncertain_threshold`; the others stay in
    /// the session as pools. Zero scores every fetched document.
    pub score_depth: usize,
    pub document_threshold: f64,
    pub uncertain_threshold: f64,
    /// Write the full audit record (raw HTTP bodies, Jev traces, document store, and
    /// classification) instead of only the report and the text files it names.
    pub full_record: bool,
    /// Folder of host-wide state that concurrent searches share (Jev provider budgets and
    /// cooldowns, source rate-limit gates). `None` keeps that state inside this process.
    pub host_dir: Option<PathBuf>,
    /// Most questions that may fetch from each named source host at once, across every process
    /// sharing `host_dir`. A host that is not named is not capped.
    pub source_slots: std::collections::BTreeMap<String, usize>,
    /// Most original pages one call reads for listing rows (at most 4; 0 reads none).
    pub original_reads: usize,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            fixture: false,
            output_dir: PathBuf::from("runs"),
            budget_usd: 0.0,
            timeout_secs: 30,
            concurrency: 16,
            jev_concurrency: 32,
            jev_hedge_ms: 2000,
            source_hedge_ms: 0,
            jev_batch: 1,
            today: crate::rank::today_utc(),
            max_pages: 2,
            max_documents: 400,
            per_source_documents: 12,
            fetch_deadline_secs: 10,
            max_body_bytes: 8 * 1024 * 1024,
            route_passes: 2,
            source_threshold: 0.2,
            fetch_threshold: 0.2,
            score_depth: 0,
            document_threshold: 0.4,
            uncertain_threshold: 0.15,
            full_record: true,
            host_dir: None,
            source_slots: Default::default(),
            original_reads: 4,
        }
    }
}

#[derive(Clone)]
pub struct FetchContext {
    pub http: crate::http::HttpRecorder,
    pub config: RunConfig,
}
