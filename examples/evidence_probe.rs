//! Compare evidence granularity on frozen source text. No retrieval or answer generation.
use anyhow::{ensure, Context, Result};
use clap::Parser;
use futures::{stream, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Instant,
};
use stellar_raven_jev::{
    evidence::{pack, text_sha256, units, Granularity, UnitScore},
    http::HttpRecorder,
    jev::JevClient,
    types::{Document, DocumentScore, RunConfig},
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    cases: PathBuf,
    /// Must be a new directory. Existing evidence is never overwritten.
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value = "dev")]
    split: String,
    #[arg(long)]
    fixture: bool,
    /// Score only complete documents. Useful for fixed-text requirement probes.
    #[arg(long)]
    document_only: bool,
    #[arg(long)]
    env_file: Option<PathBuf>,
    #[arg(long, default_value_t = 0.0)]
    budget_usd: f64,
    #[arg(long, default_value_t = 600)]
    deadline_secs: u64,
    #[arg(long, default_value_t = 1000)]
    max_requests: u64,
    #[arg(long, default_value_t = 512)]
    max_units: usize,
    #[arg(long, default_value_t = 0.4)]
    threshold: f64,
    #[arg(long, value_delimiter = ',', default_value = "512,1500,4096")]
    byte_budgets: Vec<usize>,
}

#[derive(Deserialize)]
struct Case {
    id: String,
    split: String,
    question: String,
    document: Document,
    #[serde(default)]
    required_evidence: Vec<Value>,
}

fn write(root: &Path, name: &str, value: &Value) -> Result<()> {
    let temporary = root.join(format!("{name}.tmp"));
    std::fs::write(&temporary, serde_json::to_vec_pretty(value)?)?;
    std::fs::rename(temporary, root.join(name))?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        args.threshold.is_finite() && (0.0..=1.0).contains(&args.threshold),
        "Invalid threshold"
    );
    ensure!(
        !args.byte_budgets.is_empty() && args.byte_budgets.iter().all(|n| *n > 0),
        "Invalid byte budgets"
    );
    ensure!(
        args.max_units > 0 && args.max_units <= 5000,
        "max-units must be between 1 and 5000"
    );
    ensure!(
        matches!(args.split.as_str(), "dev" | "holdout" | "all"),
        "Invalid split"
    );
    let input = std::fs::read(&args.cases)?;
    let suite: Value = serde_json::from_slice(&input)?;
    let cases: Vec<Case> = serde_json::from_value(suite.get("cases").unwrap_or(&suite).clone())?;
    let cases: Vec<_> = cases
        .into_iter()
        .filter(|c| args.split == "all" || c.split == args.split)
        .collect();
    ensure!(!cases.is_empty(), "No cases in the requested split");
    let mut ids = std::collections::BTreeSet::new();
    for case in &cases {
        ensure!(ids.insert(&case.id), "Duplicate case ID");
        ensure!(
            !case.question.trim().is_empty() && !case.document.text.trim().is_empty(),
            "Empty question or text"
        );
        for requirement in &case.required_evidence {
            let text = requirement["text"]
                .as_str()
                .context("Required evidence must contain text")?;
            ensure!(
                !text.is_empty() && case.document.text.contains(text),
                "Case {} has evidence absent from its document",
                case.id
            );
        }
    }
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
        concurrency: 8,
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
    let arms = if args.document_only {
        vec![Granularity::Document]
    } else {
        vec![
            Granularity::Document,
            Granularity::Paragraph,
            Granularity::Sentence,
            Granularity::Window,
        ]
    };
    let binary_hash = format!(
        "{:x}",
        Sha256::digest(std::fs::read(std::env::current_exe()?)?)
    );
    let mut manifest = json!({"schema_version":1,"status":"running","fixture":args.fixture,
        "binary_sha256":binary_hash,
        "compiled_source_sha256":{
            "evidence.rs":format!("{:x}", Sha256::digest(include_bytes!("../src/evidence.rs"))),
            "jev.rs":format!("{:x}", Sha256::digest(include_bytes!("../src/jev.rs"))),
            "evidence_probe.rs":format!("{:x}", Sha256::digest(include_bytes!("evidence_probe.rs")))},
        "suite_sha256":text_sha256(std::str::from_utf8(&input)?),"split":args.split,
        "case_ids":cases.iter().map(|c| &c.id).collect::<Vec<_>>(),"arms":arms,
        "threshold":args.threshold,"byte_budgets":args.byte_budgets,
        "budget_usd":args.budget_usd,"max_units_per_case":args.max_units,
        "max_requests":args.max_requests,"deadline_secs":args.deadline_secs,
        "scoring":"Existing score_document questions, unchanged supplied case question; exact identical contexts share scores within each case.",
        "limitations":["This is a conditional evidence retention experiment, not retrieval recall or answer accuracy.",
            "Required strings measure literal retention, not semantic support.",
            "Sentence splitting is heuristic. Window includes adjacent blocks, not guaranteed complete dependencies.",
            "Document arm uses existing maximum chunk score for documents above 12000 bytes.",
            "Byte budget covers source text. serialized_packet_bytes separately includes packet metadata and text.",
            "Unscored units remain unknown. A saved packet does not prove complete question support."]});
    write(&args.output, "manifest.json", &manifest)?;
    std::fs::write(args.output.join("cases.json"), &input)?;
    std::fs::create_dir(args.output.join("source-documents"))?;
    let started = Instant::now();
    let mut reports = Vec::new();
    let mut failed = false;
    for case in cases {
        let case_started = Instant::now();
        // Preserve complete provenance separately. It can contain source text, annotations,
        // or adversarial content and must not bypass the packet's source-text budget.
        let source_bytes = serde_json::to_vec_pretty(&case.document)?;
        let source_name = format!("source-documents/{:x}.json", Sha256::digest(&source_bytes));
        std::fs::write(args.output.join(&source_name), source_bytes)?;
        let mut contexts: BTreeMap<String, Document> = BTreeMap::new();
        for &arm in &arms {
            for unit in units(&case.document.text, arm) {
                let text = &case.document.text[unit.context];
                let hash = text_sha256(text);
                contexts.entry(hash.clone()).or_insert_with(|| Document {
                    id: format!("{}:{hash}", case.id),
                    text: text.into(),
                    ..case.document.clone()
                });
            }
        }
        // A cap rejects the case before inference. It never drops trailing units silently.
        if contexts.len() > args.max_units {
            reports.push(
                json!({"id":case.id,"status":"unscored","reason":"max_units exceeded",
                "planned_unique_contexts":contexts.len()}),
            );
            failed = true;
            continue;
        }
        let mut scores: BTreeMap<String, Option<DocumentScore>> = BTreeMap::new();
        let mut errors = Vec::new();
        let tasks = stream::iter(contexts.iter().map(|(hash, document)| {
            let client = &client;
            let question = &case.question;
            async move {
                (
                    hash.clone(),
                    client.score_document(question, document).await,
                )
            }
        }))
        .buffer_unordered(config.concurrency);
        futures::pin_mut!(tasks);
        while let Some((hash, outcome)) = tasks.next().await {
            match outcome {
                Ok(score) => {
                    scores.insert(hash, Some(score));
                }
                Err(error) => {
                    errors.push(json!({"context_sha256":hash,"error":error.to_string()}));
                    scores.insert(hash, None);
                    failed = true;
                }
            }
        }
        let mut arm_reports = Vec::new();
        for &arm in &arms {
            let scores: Vec<_> = units(&case.document.text, arm)
                .into_iter()
                .map(|unit| {
                    let hash = text_sha256(&case.document.text[unit.context.clone()]);
                    let probability = scores
                        .get(&hash)
                        .and_then(|s| s.as_ref())
                        .map(|s| s.probability);
                    UnitScore { unit, probability }
                })
                .collect();
            let mut packets = Vec::new();
            for budget in &args.byte_budgets {
                let packet = pack(&case.document.text, &scores, args.threshold, *budget)?;
                let texts: Vec<_> = packet
                    .spans
                    .iter()
                    .map(|span| &case.document.text[span.clone()])
                    .collect();
                let retained: Vec<_> = case.required_evidence.iter().map(|requirement| {
                    let target = requirement["text"].as_str().unwrap();
                    json!({"text":target,"retained":texts.iter().any(|text| text.contains(target))})
                }).collect();
                let presentation = json!({"document_id":case.document.id,"title":case.document.title,"url":case.document.url,
                    "source_id":case.document.source_id,
                    "content_scope":case.document.provenance.get("content_scope").or_else(|| case.document.provenance.get("content_kind")),
                    "source_document_path":source_name,
                    "packet":packet,"texts":texts});
                packets.push(json!({"byte_budget":budget,"presentation":presentation,
                    "serialized_packet_bytes":serde_json::to_vec(&presentation)?.len(),"required_evidence":retained}));
            }
            arm_reports.push(json!({"arm":arm,"scores":scores,"packets":packets}));
        }
        reports.push(
            json!({"id":case.id,"question":case.question,"split":case.split,
            "source_bytes":case.document.text.len(),"text_sha256":text_sha256(&case.document.text),
            "unique_contexts":contexts.len(),"errors":errors,"arms":arm_reports,
            "score_records":scores,"elapsed_ms":case_started.elapsed().as_millis()}),
        );
        write(&args.output, "results.json", &json!(reports))?;
        write(&args.output, "usage.json", &json!(client.usage()))?;
        write(&args.output, "http-metrics.json", &http.metrics())?;
    }
    write(&args.output, "results.json", &json!(reports))?;
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
