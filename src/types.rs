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
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Usage {
    pub requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cost_usd: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunConfig {
    pub fixture: bool,
    pub output_dir: PathBuf,
    pub budget_usd: f64,
    pub timeout_secs: u64,
    pub concurrency: usize,
    pub max_pages: usize,
    pub max_documents: usize,
    pub per_source_documents: usize,
    pub fetch_deadline_secs: u64,
    pub max_body_bytes: usize,
    pub route_passes: usize,
    pub source_threshold: f64,
    pub document_threshold: f64,
    pub uncertain_threshold: f64,
    /// Write the full audit record (raw HTTP bodies, Jev traces, document store, and
    /// classification) instead of only the report and the text files it names.
    pub full_record: bool,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            fixture: false,
            output_dir: PathBuf::from("runs"),
            budget_usd: 0.0,
            timeout_secs: 30,
            concurrency: 16,
            max_pages: 2,
            max_documents: 400,
            per_source_documents: 12,
            fetch_deadline_secs: 10,
            max_body_bytes: 8 * 1024 * 1024,
            route_passes: 2,
            source_threshold: 0.2,
            document_threshold: 0.4,
            uncertain_threshold: 0.15,
            full_record: true,
        }
    }
}

#[derive(Clone)]
pub struct FetchContext {
    pub http: crate::http::HttpRecorder,
    pub config: RunConfig,
}
