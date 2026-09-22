//! Typed retrieval plans. Queries are data; every score uses the original question.
use crate::{http::HttpRecorder, jev::JevClient, pipeline::RunOutcome, types::*};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, path::Path, time::Duration};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetrievalPlan {
    pub schema_version: u32,
    pub question: String,
    #[serde(default)]
    pub requirements: Vec<String>,
    pub bounds: Bounds,
    pub calls: Vec<Call>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bounds {
    pub max_calls: usize,
    pub max_documents: usize,
    pub max_http_requests: u64,
    pub max_response_bytes: u64,
    pub deadline_secs: u64,
    pub max_spend_usd: f64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Call {
    pub operation: String,
    pub arguments: Value,
    pub reason: String,
    pub max_documents: usize,
    pub max_pages: usize,
}

pub fn validate(plan: &RetrievalPlan) -> Result<()> {
    ensure!(plan.schema_version == 1, "Unsupported plan schema_version");
    ensure!(
        !plan.question.trim().is_empty() && plan.question.chars().count() <= 16384,
        "The original question must contain 1 to 16384 characters"
    );
    ensure!(
        serde_json::to_vec(plan)?.len() <= 256 * 1024,
        "Plan exceeds 256 KiB"
    );
    ensure!(
        plan.requirements.len() <= 64
            && plan
                .requirements
                .iter()
                .all(|r| !r.trim().is_empty() && r.chars().count() <= 2000),
        "Requirements must contain at most 64 nonempty strings of at most 2000 characters"
    );
    let b = &plan.bounds;
    for (name, value, ceiling) in [
        ("max_calls", b.max_calls as u64, 64),
        ("max_documents", b.max_documents as u64, 2000),
        ("max_http_requests", b.max_http_requests, 4096),
        (
            "max_response_bytes",
            b.max_response_bytes,
            256 * 1024 * 1024,
        ),
        ("deadline_secs", b.deadline_secs, 3600),
    ] {
        ensure!(
            value > 0 && value <= ceiling,
            "{name} must be between 1 and {ceiling}"
        );
    }
    ensure!(
        b.max_spend_usd.is_finite() && (0.0..=100.0).contains(&b.max_spend_usd),
        "max_spend_usd must be finite and between $0 and $100"
    );
    ensure!(
        !plan.calls.is_empty() && plan.calls.len() <= b.max_calls,
        "The call count exceeds the plan allowance or is empty"
    );
    for (index, call) in plan.calls.iter().enumerate() {
        ensure!(
            !call.reason.trim().is_empty() && call.reason.chars().count() <= 2000,
            "Call {index} needs a reason of at most 2000 characters"
        );
        ensure!(
            call.max_documents > 0 && call.max_documents <= b.max_documents,
            "Call {index} has an invalid document allowance"
        );
        ensure!(
            call.max_pages > 0 && call.max_pages <= 4096,
            "Call {index} has an invalid page allowance"
        );
        crate::operations::validate(&call.operation, &call.arguments)
            .with_context(|| format!("Invalid call {index}"))?;
    }
    Ok(())
}

pub fn schema() -> Value {
    json!({"type":"object","additionalProperties":false,
        "required":["schema_version","question","bounds","calls"],"properties":{
        "schema_version":{"type":"integer","const":1},
        "question":{"type":"string","minLength":1,"maxLength":16384},
        "requirements":{"type":"array","maxItems":64,"items":{"type":"string","minLength":1,"maxLength":2000}},
        "bounds":{"type":"object","additionalProperties":false,
            "required":["max_calls","max_documents","max_http_requests","max_response_bytes","deadline_secs","max_spend_usd"],
            "properties":{
                "max_calls":{"type":"integer","minimum":1,"maximum":64},
                "max_documents":{"type":"integer","minimum":1,"maximum":2000},
                "max_http_requests":{"type":"integer","minimum":1,"maximum":4096},
                "max_response_bytes":{"type":"integer","minimum":1,"maximum":268435456},
                "deadline_secs":{"type":"integer","minimum":1,"maximum":3600},
                "max_spend_usd":{"type":"number","minimum":0,"maximum":100}}},
        "calls":{"type":"array","minItems":1,"maxItems":64,"items":{
            "type":"object","additionalProperties":false,
            "required":["operation","arguments","reason","max_documents","max_pages"],"properties":{
                "operation":{"type":"string","enum":["connector.search","lumenloop.semantic","scout.projects"]},
                "arguments":{"type":"object","description":"Use the exact argument schema from list_operations."},
                "reason":{"type":"string","minLength":1,"maxLength":2000},
                "max_documents":{"type":"integer","minimum":1,"maximum":2000},
                "max_pages":{"type":"integer","minimum":1,"maximum":4096}}}}
    }})
}

fn write(root: &Path, name: &str, value: &impl Serialize) -> Result<()> {
    let target = root.join(name);
    let temporary = target.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(value)?;
    let mut file = std::fs::File::create(&temporary)?;
    std::io::Write::write_all(&mut file, &bytes)?;
    file.sync_all()?;
    std::fs::rename(temporary, target)?;
    Ok(())
}

fn identity(document: &Document) -> String {
    let bytes = serde_json::to_vec(&(
        &document.source_id,
        &document.title,
        &document.url,
        &document.text,
    ))
    .expect("String tuple serialization");
    format!("plan:{:x}", Sha256::digest(bytes))
}

#[derive(Default)]
struct Evidence {
    documents: Vec<Document>,
    retrieved: Vec<Document>,
    scores: Vec<DocumentScore>,
    omitted: Vec<Document>,
    duplicates: Vec<Value>,
    failures: Vec<Failure>,
    calls: Vec<Value>,
}
impl Evidence {
    fn failure(&mut self, stage: &str, source: Option<String>, message: impl Into<String>) {
        self.failures.push(Failure {
            stage: stage.into(),
            source_id: source,
            message: message.into(),
        });
    }
    fn persist(
        &self,
        config: &RunConfig,
        http: &HttpRecorder,
        usage: &Usage,
        status: &str,
    ) -> Result<RunOutcome> {
        let mut selected = Vec::new();
        let mut uncertain = Vec::new();
        let mut rejected = Vec::new();
        for score in &self.scores {
            if let Some(document) = self.documents.iter().find(|d| d.id == score.document_id) {
                if score.probability >= config.document_threshold {
                    selected.push(document.clone());
                } else if score.probability >= config.uncertain_threshold {
                    uncertain.push(document.clone());
                } else {
                    rejected.push(document.clone());
                }
            }
        }
        let root = &config.output_dir;
        for (name, documents) in [
            ("documents.json", &self.documents),
            ("retrieved.json", &self.retrieved),
            ("omitted.json", &self.omitted),
            ("selected.json", &selected),
            ("uncertain.json", &uncertain),
            ("rejected.json", &rejected),
        ] {
            write(root, name, documents)?;
        }
        let unscored: Vec<_> = self
            .documents
            .iter()
            .filter(|document| {
                !self
                    .scores
                    .iter()
                    .any(|score| score.document_id == document.id)
            })
            .collect();
        write(root, "unscored.json", &unscored)?;
        write(root, "scores.json", &self.scores)?;
        write(root, "failures.json", &self.failures)?;
        write(root, "duplicates.json", &self.duplicates)?;
        write(root, "calls.json", &self.calls)?;
        write(root, "usage.json", usage)?;
        write(root, "http-metrics.json", &http.metrics())?;
        let outcome = RunOutcome {
            directory: root.clone(),
            status: status.into(),
            selected: selected.len(),
            uncertain: uncertain.len(),
            rejected: rejected.len(),
            failures: self.failures.len(),
            usage: usage.clone(),
        };
        write(
            root,
            "manifest.json",
            &json!({"schema_version":1,"outcome":outcome,
            "mode":if config.fixture {"offline-fixture"} else {"live-jev"},
            "answer_generated":false,"fixture_is_model_evidence":false,"probabilities_are_calibrated":false,
            "document_count":self.documents.len(),"scored_document_count":self.scores.len(),
            "omitted_document_count":self.omitted.len(),"retrieved_document_count":self.retrieved.len(),
            "coverage_complete":false,"requirements_automatically_evaluated":false,
            "completeness":"Bounded planned retrieval. Inspect every call, provider warning, and omitted document.",
            "usage_scope":"Conservative accounting. Incomplete Jev traces retain reservations; inspect them before retrying."}),
        )?;
        Ok(outcome)
    }
}

fn exhausted(http: &HttpRecorder) -> Option<&'static str> {
    exhausted_metrics(&http.metrics())
}

fn exhausted_metrics(m: &Value) -> Option<&'static str> {
    for key in [
        "requests_started",
        "max_requests",
        "retained_response_bytes",
        "max_response_bytes",
    ] {
        if m[key].as_u64().is_none() {
            return Some("HTTP limit metrics are unavailable");
        }
    }
    if m["deadline_reached"] == true {
        return Some("The plan deadline was reached");
    }
    if m["requests_started"].as_u64()? >= m["max_requests"].as_u64()? {
        return Some("The shared HTTP request limit was reached");
    }
    if m["retained_response_bytes"].as_u64()? >= m["max_response_bytes"].as_u64()? {
        return Some("The shared response byte limit was reached");
    }
    None
}

pub async fn run_plan(plan: &RetrievalPlan, config: &RunConfig) -> Result<RunOutcome> {
    validate(plan)?;
    crate::pipeline::validate_config(config)?;
    if !config.fixture {
        ensure!(
            plan.bounds.max_spend_usd > 0.0,
            "A live plan requires a positive spending allocation"
        );
        ensure!(
            plan.bounds.max_spend_usd <= config.budget_usd,
            "The plan spending allowance exceeds --budget-usd"
        );
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(plan.bounds.deadline_secs);
    let mut config = config.clone();
    config.budget_usd = if config.fixture {
        0.0
    } else {
        plan.bounds.max_spend_usd
    };
    config.max_documents = plan.bounds.max_documents;
    config.output_dir = config
        .output_dir
        .join(format!("plan-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(
        config
            .output_dir
            .parent()
            .context("Missing output parent")?,
    )?;
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(&config.output_dir)?;
    std::fs::create_dir(config.output_dir.join("fetches"))?;
    write(&config.output_dir, "plan.json", plan)?;
    write(
        &config.output_dir,
        "question.json",
        &json!({"question":plan.question,"requirements":plan.requirements,"config":config}),
    )?;
    let http = HttpRecorder::new_bounded(
        &config.output_dir,
        &config,
        plan.bounds.max_http_requests,
        plan.bounds.max_response_bytes,
        plan.bounds.deadline_secs,
    )?;
    let mut evidence = Evidence::default();
    evidence.persist(&config, &http, &Usage::default(), "running")?;
    let jev = match JevClient::new(&config, &http) {
        Ok(jev) => jev,
        Err(error) => {
            evidence.failure("plan.authentication", None, error.to_string());
            let result = evidence.persist(&config, &http, &Usage::default(), "failed")?;
            crate::export::write_run_index(&config.output_dir)?;
            return Ok(result);
        }
    };
    let mut seen = BTreeSet::new();
    for (index, call) in plan.calls.iter().enumerate() {
        let mut receipt = json!({"index":index,"call":call,"state":"running"});
        if let Some(reason) = exhausted(&http).or_else(|| {
            (tokio::time::Instant::now() >= deadline).then_some("The plan deadline was reached")
        }) {
            receipt["state"] = json!("skipped_limit");
            receipt["reason"] = json!(reason);
            evidence.calls.push(receipt);
            evidence.failure("plan.limit", None, reason);
            evidence.persist(&config, &http, &jev.usage(), "running")?;
            continue;
        }
        evidence.calls.push(receipt);
        evidence.persist(&config, &http, &jev.usage(), "running")?;
        let mut call_config = config.clone();
        call_config.max_documents = call.max_documents;
        call_config.per_source_documents = call.max_documents;
        call_config.max_pages = call.max_pages;
        let context = FetchContext {
            http: http.clone(),
            config: call_config,
        };
        // The recorder ends network waits at the deadline. Let adapters return
        // their completed pages instead of canceling their accumulated results.
        let fetched =
            match crate::operations::fetch(&context, &call.operation, &call.arguments).await {
                Ok(fetched) => fetched,
                Err(error) => {
                    evidence.calls[index]["state"] = json!("failed");
                    evidence.calls[index]["error"] = json!(error.to_string());
                    evidence.failure("plan.fetch", None, format!("Call {index}: {error}"));
                    evidence.persist(&config, &http, &jev.usage(), "running")?;
                    continue;
                }
            };
        let artifact = format!("fetches/{index:03}.json");
        write(&config.output_dir, &artifact, &fetched)?;
        evidence.calls[index]["state"] = json!("fetched");
        evidence.calls[index]["artifact"] = json!(artifact);
        evidence.calls[index]["retrieved"] = json!(fetched.documents.len());
        evidence.calls[index]["failures"] = json!(fetched.failures.len());
        evidence
            .failures
            .extend(fetched.failures.into_iter().map(|mut failure| {
                failure.message = format!("Call {index}: {}", failure.message);
                failure
            }));
        evidence.retrieved.extend(fetched.documents.iter().cloned());
        evidence.persist(&config, &http, &jev.usage(), "running")?;
        let mut added = 0;
        for mut document in fetched.documents {
            let original_id = document.id.clone();
            document.id = identity(&document);
            if !document.provenance.is_object() {
                document.provenance = json!({"source_provenance":document.provenance});
            }
            document.provenance["plan_receipt"] = json!({"call_index":index,"provider_document_id":original_id,"scoring_id":document.id});
            if !seen.insert(document.id.clone()) {
                evidence.duplicates.push(
                    json!({"call_index":index,"original_id":original_id,"document":document}),
                );
                continue;
            }
            let limit = exhausted(&http).or_else(|| {
                (tokio::time::Instant::now() >= deadline).then_some("The plan deadline was reached")
            });
            if document.text.trim().is_empty()
                || evidence.documents.len() >= plan.bounds.max_documents
                || limit.is_some()
            {
                let reason = limit.unwrap_or(if document.text.trim().is_empty() {
                    "The document has no text"
                } else {
                    "The global document admission limit was reached"
                });
                evidence.failure("plan.omitted", Some(document.source_id.clone()), reason);
                evidence.omitted.push(document);
                continue;
            }
            evidence.documents.push(document.clone());
            evidence.persist(&config, &http, &jev.usage(), "running")?;
            match tokio::time::timeout_at(deadline, jev.score_document(&plan.question, &document))
                .await
            {
                Ok(Ok(score)) => {
                    if score.document_id == document.id && score.probability.is_finite() && (0.0..=1.0).contains(&score.probability) {
                        evidence.scores.push(score);
                        added += 1;
                    } else {
                        evidence.failure("plan.score", Some(document.source_id.clone()), format!("Call {index}: Invalid document score for {}", document.id));
                    }
                }
                Ok(Err(error)) => evidence.failure(
                    "plan.score",
                    Some(document.source_id.clone()),
                    format!("Call {index}, document {}: {error}", document.id),
                ),
                Err(_) => evidence.failure(
                    "plan.deadline",
                    Some(document.source_id.clone()),
                    format!("Call {index}, document {}: Scoring stopped at the deadline; a paid request may remain unresolved", document.id),
                ),
            }
            evidence.persist(&config, &http, &jev.usage(), "running")?;
        }
        evidence.calls[index]["state"] = json!("finished");
        evidence.calls[index]["new_scored_documents"] = json!(added);
        evidence.persist(&config, &http, &jev.usage(), "running")?;
    }
    let status = if evidence.failures.is_empty() {
        "complete"
    } else {
        "partial"
    };
    let outcome = evidence.persist(&config, &http, &jev.usage(), status)?;
    crate::export::write_run_index(&config.output_dir)?;
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unavailable_metrics_stop_work() {
        assert!(exhausted_metrics(&json!({"bounded":false})).is_some());
        assert!(exhausted_metrics(&json!({"requests_started":0,"max_requests":"10","retained_response_bytes":0,"max_response_bytes":100})).is_some());
    }

    #[test]
    fn identity_retains_changed_content_even_when_provider_reuses_ids() {
        let mut d = Document {
            id: "provider-id".into(),
            source_id: "source".into(),
            title: "title".into(),
            url: "url".into(),
            text: "first".into(),
            provenance: Value::Null,
            raw_artifacts: vec![],
        };
        let original = identity(&d);
        d.text = "second".into();
        assert_ne!(original, identity(&d));
        d.text = "first".into();
        d.id = "different-id".into();
        assert_eq!(original, identity(&d));
        d.url = "another-url".into();
        assert_ne!(original, identity(&d));
    }
}
