//! Bounded experimental scoring of saved public contexts. No source retrieval.
use anyhow::{ensure, Context, Result};
use clap::Parser;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    time::Instant,
};
use stellar_raven_jev::{http::HttpRecorder, jev::JevClient, types::RunConfig};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    jobs: PathBuf,
    #[arg(long)]
    input_freeze: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    fixture: bool,
    #[arg(long)]
    env_file: Option<PathBuf>,
    #[arg(long, default_value_t = 0.0)]
    budget_usd: f64,
    #[arg(long, default_value_t = 400)]
    max_requests: u64,
    #[arg(long, default_value_t = 600)]
    deadline_secs: u64,
}
#[derive(Deserialize)]
struct Suite {
    jobs: Vec<Job>,
    sources: BTreeMap<String, Value>,
}
#[derive(Deserialize)]
struct Requirement {
    id: String,
    text: String,
}
#[derive(Deserialize)]
struct Job {
    id: String,
    question: String,
    requirements: Vec<Requirement>,
    context: Value,
    source_text_sha256: String,
    context_sha256: String,
    source_key: String,
    spans: Vec<Span>,
}

#[derive(Deserialize)]
struct Span {
    start: usize,
    end: usize,
}

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn write(root: &Path, name: &str, value: &Value) -> Result<()> {
    let temporary = root.join(format!("{name}.tmp"));
    std::fs::write(&temporary, serde_json::to_vec_pretty(value)?)?;
    std::fs::rename(temporary, root.join(name))?;
    Ok(())
}
fn validate(suite: &Suite) -> Result<()> {
    ensure!(!suite.jobs.is_empty(), "No jobs");
    let mut ids = BTreeSet::new();
    for job in &suite.jobs {
        ensure!(
            !job.id.is_empty() && ids.insert(&job.id),
            "Duplicate or empty job ID"
        );
        let text = job.context["text"]
            .as_str()
            .context("Missing context text")?;
        ensure!(
            hash(text.as_bytes()) == job.context_sha256,
            "Context hash mismatch"
        );
        ensure!(
            job.source_text_sha256.len() == 64
                && job
                    .source_text_sha256
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit()),
            "Invalid source hash"
        );
        let source = suite
            .sources
            .get(&job.source_key)
            .context("Missing source binding")?;
        let source_text = source["text"]
            .as_str()
            .context("Missing bound source text")?;
        ensure!(
            hash(source_text.as_bytes()) == job.source_text_sha256
                && source["text_sha256"] == job.source_text_sha256,
            "Bound source hash mismatch"
        );
        for (left, right) in [
            ("source_id", "id"),
            ("title", "title"),
            ("url", "url"),
            ("source_content_scope", "content_scope"),
        ] {
            ensure!(
                job.context[left] == source[right],
                "Context/source field mismatch: {left}"
            );
        }
        let empty = json!({});
        ensure!(
            job.context.get("metadata").unwrap_or(&empty)
                == source.get("metadata").unwrap_or(&empty),
            "Context metadata mismatch"
        );
        let mut pieces = Vec::new();
        let mut previous_end = 0;
        for span in &job.spans {
            ensure!(
                span.start >= previous_end && span.start < span.end,
                "Invalid context range order"
            );
            pieces.push(
                source_text
                    .get(span.start..span.end)
                    .context("Invalid UTF-8 source range")?,
            );
            previous_end = span.end;
        }
        ensure!(
            !pieces.is_empty() && pieces.join("\n[…]\n") == text,
            "Context ranges do not reproduce text"
        );
        let scope = if text == source_text {
            "full"
        } else {
            "excerpt"
        };
        ensure!(
            job.context["delivered_scope"] == scope,
            "Delivered scope mismatch"
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        args.budget_usd.is_finite() && (0.0..=0.10).contains(&args.budget_usd),
        "Budget must be within this round's $0.10 cap"
    );
    ensure!(
        args.max_requests > 0
            && args.max_requests <= 400
            && args.deadline_secs > 0
            && args.deadline_secs <= 600,
        "Round bound exceeded"
    );
    let input = std::fs::read(&args.jobs)?;
    let freeze: Value = serde_json::from_slice(&std::fs::read(&args.input_freeze)?)?;
    let root = args
        .input_freeze
        .parent()
        .context("Input freeze has no parent")?;
    ensure!(
        args.jobs.canonicalize()? == root.join("jobs.json").canonicalize()?,
        "Jobs path does not belong to input freeze"
    );
    let entries = freeze["sha256"]
        .as_object()
        .context("Missing freeze hashes")?;
    ensure!(
        entries.contains_key("jobs.json"),
        "Freeze does not bind jobs"
    );
    for (name, expected) in entries {
        let relative = Path::new(name);
        ensure!(
            relative
                .components()
                .all(|p| matches!(p, std::path::Component::Normal(_))),
            "Unsafe freeze path"
        );
        ensure!(
            expected.as_str() == Some(hash(&std::fs::read(root.join(relative))?).as_str()),
            "Frozen input changed: {name}"
        );
    }
    ensure!(
        freeze["sha256"]["jobs.json"] == hash(&input),
        "Frozen jobs changed during validation"
    );
    let suite: Suite = serde_json::from_slice(&input)?;
    validate(&suite)?;
    // Validate every inference input before initialization or any paid call.
    for job in &suite.jobs {
        let requirements = job
            .requirements
            .iter()
            .map(|r| (r.id.clone(), r.text.clone()))
            .collect::<Vec<_>>();
        JevClient::validate_context_input(&job.question, &requirements, &job.context)?;
    }
    if !args.fixture {
        let env = args
            .env_file
            .as_ref()
            .context("Live scoring requires --env-file")?;
        ensure!(env.is_absolute(), "--env-file must be absolute");
        dotenvy::from_path(env).context("Cannot load explicit environment file")?;
    }
    ensure!(!args.output.exists(), "Output already exists");
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
        concurrency: 1,
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
    let mut manifest = json!({"schema_version":1,"status":"running","fixture":args.fixture,
        "binary_sha256":hash(&std::fs::read(std::env::current_exe()?)?),
        "compiled_source_sha256":{"jev.rs":hash(include_bytes!("../src/jev.rs")),"context_probe.rs":hash(include_bytes!("context_probe.rs"))},
        "input_sha256":hash(&input),"requested":suite.jobs.len(),"budget_usd":args.budget_usd,
        "input_freeze_sha256":hash(&std::fs::read(&args.input_freeze)?),
        "max_requests":args.max_requests,"deadline_secs":args.deadline_secs,
        "failure_policy":"Stop new jobs on first failure. Keep reservation and raw response. No process resume or automatic rerun.",
        "limits":["No source retrieval", "Uncalibrated independent signals", "Fixture scores are synthetic zeros",
        "Reservations bound planned spend. An invalid oversized provider receipt remains recorded in full and stops the run."]});
    write(&args.output, "manifest.json", &manifest)?;
    std::fs::write(args.output.join("jobs.json"), &input)?;
    let started = Instant::now();
    let mut rows = Vec::new();
    let mut stop_reason = None;
    for job in &suite.jobs {
        let mut row = json!({"id":job.id,"source_text_sha256":job.source_text_sha256,"context_sha256":job.context_sha256});
        if let Some(reason) = &stop_reason {
            row["status"] = json!("not_run");
            row["reason"] = json!(reason);
        } else {
            let requirements = job
                .requirements
                .iter()
                .map(|r| (r.id.clone(), r.text.clone()))
                .collect::<Vec<_>>();
            let call_started = Instant::now();
            match client
                .score_context(&job.question, &requirements, &job.context)
                .await
            {
                Ok((signals, audit)) => {
                    let mut scores = serde_json::Map::new();
                    for (index, requirement) in job.requirements.iter().enumerate() {
                        let mut values = serde_json::Map::new();
                        for key in ["useful", "sufficient", "conflict"] {
                            values.insert(key.into(), json!(signals[&format!("r{index}_{key}")]));
                        }
                        scores.insert(requirement.id.clone(), json!(values));
                    }
                    row["status"] = json!("complete");
                    row["scores"] = json!(scores);
                    row["audit"] = json!(audit);
                }
                Err(error) => {
                    row["status"] = json!("failed");
                    row["error"] = json!(error.to_string());
                    stop_reason = Some(format!(
                        "Previous job {} failed; inspect raw accounting before another attempt",
                        job.id
                    ));
                }
            }
            row["elapsed_ms"] = json!(call_started.elapsed().as_millis());
        }
        rows.push(row);
        write(&args.output, "results.json", &json!(rows))?;
        write(&args.output, "usage.json", &json!(client.usage()))?;
        write(&args.output, "http-metrics.json", &http.metrics())?;
    }
    manifest["status"] = json!(if stop_reason.is_some() {
        "partial"
    } else {
        "complete"
    });
    manifest["elapsed_ms"] = json!(started.elapsed().as_millis());
    manifest["usage"] = json!(client.usage());
    manifest["http_metrics"] = http.metrics();
    write(&args.output, "manifest.json", &manifest)?;
    println!("{}", serde_json::to_string(&manifest)?);
    if stop_reason.is_some() {
        std::process::exit(2);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input() -> Value {
        let text = "A fee is 7.\n\nA vote follows.";
        let context = "A fee is 7.";
        json!({"sources":{"key":{"id":"s1","title":"Fees","url":"https://example.test","content_scope":"full",
            "text":text,"text_sha256":hash(text.as_bytes()),"metadata":{"publishedAt":"2025-01-01"}}},
            "jobs":[{"id":"job1","question":"What is the fee?","requirements":[{"id":"r1","text":"State the fee"}],
                "source_key":"key","source_text_sha256":hash(text.as_bytes()),"context_sha256":hash(context.as_bytes()),
                "spans":[{"start":0,"end":context.len()}],
                "context":{"source_id":"s1","title":"Fees","url":"https://example.test","source_content_scope":"full",
                    "delivered_scope":"excerpt","text":context,"metadata":{"publishedAt":"2025-01-01"}}}]})
    }

    #[test]
    fn source_binding_refuses_changed_text_ranges_metadata_and_scope() {
        let good = input();
        validate(&serde_json::from_value(good.clone()).unwrap()).unwrap();
        for field in ["text", "ranges", "metadata", "scope", "source_id", "hash"] {
            let mut bad = good.clone();
            match field {
                "text" => bad["sources"]["key"]["text"] = json!("changed"),
                "ranges" => bad["jobs"][0]["spans"][0]["end"] = json!(3),
                "metadata" => {
                    bad["jobs"][0]["context"]["metadata"]["publishedAt"] = json!("2026-01-01")
                }
                "scope" => bad["jobs"][0]["context"]["delivered_scope"] = json!("full"),
                "source_id" => bad["jobs"][0]["context"]["source_id"] = json!("s2"),
                "hash" => bad["jobs"][0]["source_text_sha256"] = json!("0".repeat(64)),
                _ => unreachable!(),
            }
            assert!(
                validate(&serde_json::from_value(bad).unwrap()).is_err(),
                "{field}"
            );
        }
    }
}
