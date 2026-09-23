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
    /// Parsed documents explicitly omitted by an adapter, not unreturned provider rows.
    #[serde(default)]
    pub omitted_documents: Vec<Document>,
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
    /// Every Jev signal for the document. Empty for older runs and fixtures.
    #[serde(default)]
    pub signals: std::collections::BTreeMap<String, f64>,
    /// How `signals` was built. `independent_max_per_signal_across_chunks` means each value is the
    /// maximum of that signal over the document's chunks; different signals can come from different
    /// chunks, so the map is not one jointly supported evidence vector.
    #[serde(default)]
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
    #[serde(default = "default_per_source_documents")]
    pub per_source_documents: usize,
    #[serde(default = "default_fetch_deadline_secs")]
    pub fetch_deadline_secs: u64,
    pub max_body_bytes: usize,
    pub route_passes: usize,
    pub source_threshold: f64,
    pub document_threshold: f64,
    pub uncertain_threshold: f64,
}

fn default_per_source_documents() -> usize {
    12
}

fn default_fetch_deadline_secs() -> u64 {
    10
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
            per_source_documents: default_per_source_documents(),
            fetch_deadline_secs: default_fetch_deadline_secs(),
            max_body_bytes: 8 * 1024 * 1024,
            route_passes: 2,
            source_threshold: 0.2,
            document_threshold: 0.4,
            uncertain_threshold: 0.15,
        }
    }
}

#[derive(Clone)]
pub struct FetchContext {
    pub http: crate::http::HttpRecorder,
    pub config: RunConfig,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn old_run_config_defaults_per_source_documents_to_twelve() {
        let mut value = serde_json::to_value(RunConfig::default()).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .remove("per_source_documents");
        let config: RunConfig = serde_json::from_value(value).unwrap();
        assert_eq!(config.per_source_documents, 12);
    }
}
