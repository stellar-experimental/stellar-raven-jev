//! Compare document usefulness with direct claim support. No source retrieval.
//! The calibration file shape is `{cases:[{id,claim,evidence:[{id,title,url,text}],cited_source_ids}]}`.
//! This probe builds the `{sources,cited_source_ids}` object used by `score_claim_support`.
//! Private label fields are ignored and are not sent to inference.
use anyhow::{ensure, Context, Result};
use clap::Parser;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::Instant,
};
use stellar_raven_jev::{
    http::HttpRecorder,
    jev::JevClient,
    types::{Document, RunConfig},
};

const PUBLIC_FIELDS: &[&str] = &["id", "title", "url", "text"];

#[derive(Parser)]
struct Args {
    /// Calibration file. Use `eval-next/calibration-cases.json` for the planned set.
    #[arg(long)]
    cases: PathBuf,
    /// Must be a new directory. Existing output is never overwritten.
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    fixture: bool,
    #[arg(long)]
    env_file: Option<PathBuf>,
    #[arg(long, default_value_t = 0.0)]
    budget_usd: f64,
    #[arg(long, default_value_t = 100)]
    max_requests: u64,
    #[arg(long, default_value_t = 300)]
    deadline_secs: u64,
}

#[derive(Deserialize)]
struct Suite {
    cases: Vec<CaseFile>,
}

#[derive(Deserialize)]
struct CaseFile {
    id: String,
    claim: String,
    evidence: Vec<PublicSource>,
    cited_source_ids: Vec<String>,
}

#[derive(Deserialize)]
struct PublicSource {
    id: String,
    title: String,
    url: String,
    text: String,
}

fn baseline_question(claim: &str) -> String {
    format!("Does the cited evidence support this claim: {claim}?")
}

fn project_source(source: &PublicSource) -> Value {
    let mut out = Map::new();
    for key in PUBLIC_FIELDS {
        let value = match *key {
            "id" => source.id.as_str(),
            "title" => source.title.as_str(),
            "url" => source.url.as_str(),
            "text" => source.text.as_str(),
            _ => continue,
        };
        out.insert((*key).to_string(), Value::String(value.to_string()));
    }
    Value::Object(out)
}

fn project_evidence(case: &CaseFile) -> Result<Value> {
    let mut sources = Vec::new();
    let mut seen = BTreeSet::new();
    for source in &case.evidence {
        ensure!(
            !source.id.is_empty() && seen.insert(source.id.clone()),
            "Evidence source ids must be nonempty and unique"
        );
        ensure!(
            !source.text.trim().is_empty(),
            "Evidence source text is empty"
        );
        sources.push(project_source(source));
    }
    for id in &case.cited_source_ids {
        ensure!(seen.contains(id), "Cited source {id} is not in sources");
    }
    Ok(json!({
        "sources": sources,
        "cited_source_ids": case.cited_source_ids
    }))
}

fn cited_document(case: &CaseFile, evidence: &Value) -> Document {
    let cited: BTreeSet<_> = case.cited_source_ids.iter().map(String::as_str).collect();
    let mut text = String::new();
    for source in evidence["sources"].as_array().unwrap() {
        let id = source["id"].as_str().unwrap();
        if !cited.contains(id) {
            continue;
        }
        text.push_str(&format!("Source id: {id}\n"));
        if let Some(title) = source.get("title").and_then(Value::as_str) {
            text.push_str(&format!("Title: {title}\n"));
        }
        if let Some(url) = source.get("url").and_then(Value::as_str) {
            text.push_str(&format!("URL: {url}\n"));
        }
        text.push('\n');
        text.push_str(source["text"].as_str().unwrap());
        text.push_str("\n\n");
    }
    Document {
        id: case.id.clone(),
        source_id: "calibration".into(),
        title: "Cited evidence".into(),
        url: String::new(),
        text,
        provenance: Value::Null,
        raw_artifacts: Vec::new(),
    }
}

fn load_suite(bytes: &[u8]) -> Result<Suite> {
    let suite: Suite = serde_json::from_slice(bytes).context("Cannot read calibration cases")?;
    ensure!(!suite.cases.is_empty(), "No calibration cases");
    let mut ids = BTreeSet::new();
    for case in &suite.cases {
        ensure!(ids.insert(case.id.as_str()), "Duplicate case ID");
        ensure!(
            !case.claim.trim().is_empty(),
            "Case {} has an empty claim",
            case.id
        );
        ensure!(
            !case.evidence.is_empty(),
            "Case {} has no evidence",
            case.id
        );
        project_evidence(case)?;
    }
    Ok(suite)
}

fn write(root: &Path, name: &str, value: &Value) -> Result<()> {
    let temporary = root.join(format!("{name}.tmp"));
    std::fs::write(&temporary, serde_json::to_vec_pretty(value)?)?;
    std::fs::rename(temporary, root.join(name))?;
    Ok(())
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        args.budget_usd.is_finite() && args.budget_usd >= 0.0,
        "Budget must be finite and nonnegative"
    );
    ensure!(
        args.max_requests > 0 && args.deadline_secs > 0,
        "Request cap and deadline must be positive"
    );
    let input = std::fs::read(&args.cases)?;
    let suite = load_suite(&input)?;
    if !args.fixture {
        let env = args
            .env_file
            .as_ref()
            .context("Live scoring requires --env-file")?;
        ensure!(env.is_absolute(), "--env-file must be absolute");
        dotenvy::from_path(env).context("Cannot load the explicit environment file")?;
    }
    ensure!(!args.output.exists(), "Output directory already exists");
    std::fs::create_dir_all(args.output.parent().unwrap_or(Path::new(".")))?;
    std::fs::create_dir(&args.output)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&args.output, std::fs::Permissions::from_mode(0o700))?;
    }
    let config = RunConfig {
        fixture: args.fixture,
        budget_usd: args.budget_usd,
        output_dir: args.output.clone(),
        timeout_secs: args.deadline_secs,
        concurrency: 4,
        ..RunConfig::default()
    };
    let http = HttpRecorder::new_bounded(
        &args.output,
        &config,
        args.max_requests,
        64 * 1024 * 1024,
        args.deadline_secs,
    )?;
    let client = JevClient::new(&config, &http)?;
    let binary_hash = sha256_bytes(&std::fs::read(std::env::current_exe()?)?);
    let mut manifest = json!({
        "schema_version": 1,
        "status": "running",
        "fixture": args.fixture,
        "binary_sha256": binary_hash,
        "compiled_source_sha256": {
            "jev.rs": sha256_bytes(include_bytes!("../src/jev.rs")),
            "support_probe.rs": sha256_bytes(include_bytes!("support_probe.rs"))
        },
        "input_sha256": sha256_bytes(&input),
        "case_ids": suite.cases.iter().map(|c| &c.id).collect::<Vec<_>>(),
        "public_fields": PUBLIC_FIELDS,
        "budget_usd": args.budget_usd,
        "max_requests": args.max_requests,
        "deadline_secs": args.deadline_secs,
        "diagnostic_threshold": 0.5,
        "scoring": "score_document uses the fixed cited-evidence question. score_claim_support is a separate experimental call. Raw scores are not calibrated.",
        "limitations": [
            "No source retrieval.",
            "Private labels are not loaded into inference.",
            "Document usefulness is not a support classifier.",
            "The 0.5 value is a recorded diagnostic threshold, not a calibrated cutoff.",
            "Fixture scores are synthetic and do not judge the claim."
        ]
    });
    write(&args.output, "manifest.json", &manifest)?;
    let started = Instant::now();
    let mut reports = Vec::new();
    let mut public_cases = Vec::new();
    let mut failed = false;
    for case in &suite.cases {
        let case_started = Instant::now();
        let evidence = match project_evidence(case) {
            Ok(evidence) => evidence,
            Err(error) => {
                failed = true;
                reports.push(json!({
                    "id": case.id,
                    "errors": [{"arm":"prepare","error": error.to_string()}],
                    "elapsed_ms": case_started.elapsed().as_millis()
                }));
                continue;
            }
        };
        let document = cited_document(case, &evidence);
        let question = baseline_question(&case.claim);
        public_cases.push(json!({
            "id": case.id,
            "claim": case.claim,
            "baseline_question": question,
            "evidence": evidence
        }));
        let mut errors = Vec::new();
        let baseline = match client.score_document(&question, &document).await {
            Ok(score) => json!({
                "question": question,
                "document_id": score.document_id,
                "probability": score.probability,
                "signals": score.signals,
                "signals_aggregation": score.signals_aggregation,
                "reason": score.reason
            }),
            Err(error) => {
                failed = true;
                errors.push(json!({"arm":"score_document","error": error.to_string()}));
                Value::Null
            }
        };
        let claim_support = match client.score_claim_support(&case.claim, &evidence).await {
            Ok((signals, audit)) => json!({
                "signals": signals,
                "audit": audit,
                "synthetic": args.fixture
            }),
            Err(error) => {
                failed = true;
                errors.push(json!({"arm":"score_claim_support","error": error.to_string()}));
                Value::Null
            }
        };
        reports.push(json!({
            "id": case.id,
            "claim": case.claim,
            "baseline": baseline,
            "claim_support": claim_support,
            "errors": errors,
            "elapsed_ms": case_started.elapsed().as_millis()
        }));
        write(&args.output, "results.json", &json!(reports))?;
        write(&args.output, "usage.json", &json!(client.usage()))?;
        write(&args.output, "http-metrics.json", &http.metrics())?;
    }
    write(&args.output, "public-cases.json", &json!(public_cases))?;
    write(&args.output, "results.json", &json!(reports))?;
    write(&args.output, "usage.json", &json!(client.usage()))?;
    write(&args.output, "http-metrics.json", &http.metrics())?;
    manifest["status"] = json!(if failed { "partial" } else { "complete" });
    manifest["elapsed_ms"] = json!(started.elapsed().as_millis());
    manifest["usage"] = json!(client.usage());
    manifest["http_metrics"] = http.metrics();
    write(&args.output, "manifest.json", &manifest)?;
    println!("{}", serde_json::to_string(&manifest)?);
    if failed {
        std::process::exit(2);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_schema_projects_public_fields_and_drops_private_labels() {
        let marker = "PRIVATE_LABEL_MARKER_7f3a";
        let raw = json!({
            "cases": [{
                "id": "c1",
                "claim": "The cap is 200.",
                "label": marker,
                "evidence": [{
                    "id": "s1",
                    "title": "Limits",
                    "url": "https://example.test/limits",
                    "text": "The cap is 200 entries.",
                    "label": marker
                }, {
                    "id": "s2",
                    "title": "Other",
                    "url": "https://example.test/other",
                    "text": "This source is not cited.",
                    "gold": marker
                }],
                "cited_source_ids": ["s1"]
            }]
        });
        let suite = load_suite(&serde_json::to_vec(&raw).unwrap()).unwrap();
        let evidence = project_evidence(&suite.cases[0]).unwrap();
        let encoded = serde_json::to_string(&evidence).unwrap();
        assert!(!encoded.contains(marker));
        assert_eq!(
            evidence
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["cited_source_ids".into(), "sources".into()])
        );
        assert_eq!(
            evidence["sources"][0]
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["id".into(), "text".into(), "title".into(), "url".into()])
        );
        assert_eq!(evidence["cited_source_ids"][0], "s1");
        assert_eq!(
            baseline_question(&suite.cases[0].claim),
            "Does the cited evidence support this claim: The cap is 200.?"
        );
        assert!(load_suite(
            &serde_json::to_vec(&json!({
                "cases": [{
                    "id": "c1",
                    "claim": "The cap is 200.",
                    "evidence": {"sources": [], "cited_source_ids": []},
                    "cited_source_ids": []
                }]
            }))
            .unwrap()
        )
        .is_err());
    }
}
