//! Bounded live retrieval probe.
//!
//! Owns no shared crate code. Calls existing connectors and one shared `JevClient`.
//! Read-only source calls only. The original question is the only Jev scoring text.
//! Bounds: $2, 160 unique documents, 3 pages per fetch, 20 source fetches.

use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use stellar_raven_jev::connectors;
use stellar_raven_jev::http::HttpRecorder;
use stellar_raven_jev::jev::JevClient;
use stellar_raven_jev::query::{plan, VariantKind};
use stellar_raven_jev::types::{Document, FetchContext, RunConfig, Source};

const MAX_FETCHES: usize = 20;
const MAX_PAGES: usize = 3;
const MAX_UNIQUE: usize = 160;
const BUDGET_USD: f64 = 2.0;
const SCORE_STOP_USD: f64 = 1.85;
const SCORE_WALL_SECS: u64 = 1500;
const EQUAL_PAGES: usize = 1;
const EQUAL_DOCS: usize = 4;
const VARIANT_PAGES: usize = 1;
const VARIANT_DOCS: usize = 4;
const FETCH_TIMEOUT_SECS: u64 = 420;

struct QuestionDef {
    id: &'static str,
    text: &'static str,
    equal_sources: &'static [&'static str],
    focused_source: &'static str,
    focused_pages: usize,
    focused_documents: usize,
    variant_source: &'static str,
    focused_note: &'static str,
}

const QUESTIONS: &[QuestionDef] = &[
    QuestionDef {
        id: "q-archive",
        text: "How do I restore archived Soroban contract storage?",
        equal_sources: &[
            "algolia:docs:primary",
            "lumenloop.research",
            "stellarlight.research.dev-docs",
        ],
        focused_source: "algolia:docs:primary",
        focused_pages: 3,
        focused_documents: 12,
        variant_source: "algolia:docs:primary",
        focused_note: "Docs production index. One facet, so extra pages paginate the same keyword text. Page size equals max_documents in this connector.",
    },
    QuestionDef {
        id: "q-getevents",
        text: "How does the Stellar RPC getEvents method use cursor pagination for historical events?",
        equal_sources: &[
            "algolia:docs:primary",
            "lumenloop.research",
            "stellarlight.research.dev-docs",
        ],
        focused_source: "algolia:docs:primary",
        focused_pages: 3,
        focused_documents: 12,
        variant_source: "lumenloop.research",
        focused_note: "Docs production index. One clause keeps Algolia off independent-facet mode. Lumenloop variants are a separate arm.",
    },
    QuestionDef {
        id: "q-sep",
        text: "Find sources that explain SEP-10 authentication and how SEP-24 uses the resulting token.",
        equal_sources: &[
            "algolia:docs:primary",
            "lumenloop.research",
            "stellarlight.research.sep",
        ],
        focused_source: "stellarlight.research.sep",
        focused_pages: 1,
        focused_documents: 12,
        variant_source: "stellarlight.research.sep",
        focused_note: "Scout source=sep is the exact source filter. Research has no offset. Depth is the limit. Algolia max_pages>=2 would switch this two-facet question to independent facet queries, so the focused arm does not raise Algolia pages.",
    },
];

#[derive(Clone)]
struct FetchSpec {
    id: String,
    question_id: String,
    original_question: String,
    source_id: String,
    arm: String,
    fetch_question: String,
    variant_kind: String,
    max_pages: usize,
    max_documents: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let plans_only = std::env::args().any(|arg| arg == "--plans-only");
    if !plans_only {
        dotenvy::dotenv().ok();
    }
    let empty = sha256_hex(b"").context("SHA-256 check failed")?;
    anyhow::ensure!(
        empty == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "SHA-256 empty digest mismatch"
    );

    let registry = connectors::sources();
    let (schedule, dropped) = build_schedule(&registry)?;
    anyhow::ensure!(schedule.len() <= MAX_FETCHES, "Schedule exceeds 20 fetches");
    anyhow::ensure!(
        schedule
            .iter()
            .all(|item| item.max_pages <= MAX_PAGES && item.max_pages > 0),
        "Page bound violated"
    );

    let root = if plans_only {
        PathBuf::from("experiments/query-probes/plan-check")
    } else {
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        PathBuf::from("experiments/query-probes").join(format!("{stamp}-{}", uuid::Uuid::new_v4()))
    };
    std::fs::create_dir_all(&root)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
    }

    write_json(
        &root.join("design.json"),
        &json!({
            "bounds": {
                "budget_usd": BUDGET_USD,
                "score_stop_usd": SCORE_STOP_USD,
                "score_wall_secs": SCORE_WALL_SECS,
                "max_unique_documents": MAX_UNIQUE,
                "max_pages_per_fetch": MAX_PAGES,
                "max_source_fetches": MAX_FETCHES,
                "fetch_timeout_secs": FETCH_TIMEOUT_SECS
            },
            "jev_scoring_question": "original question text for every score_document call",
            "route_called": false,
            "route_reason": "Sources are fixed from the registry. route() was not called. The budget is reserved for document scores.",
            "money_movement": "none. Existing read connectors and Jev scoring only.",
            "questions": QUESTIONS.iter().map(|q| json!({
                "id": q.id,
                "text": q.text,
                "equal_sources": q.equal_sources,
                "focused_source": q.focused_source,
                "focused_pages": q.focused_pages,
                "focused_documents": q.focused_documents,
                "variant_source": q.variant_source,
                "focused_note": q.focused_note
            })).collect::<Vec<_>>(),
            "schedule": schedule.iter().map(spec_json).collect::<Vec<_>>(),
            "dropped_variants": dropped,
            "comparisons": [
                "equal_depth: each preferred source, max_pages=1, max_documents=4, original question",
                "focused_depth: one source, larger document cap, original question",
                "query_variant: same source as one prose fetch, planner semantic facets then keyword variants, small cap"
            ]
        }),
    )?;
    for question in QUESTIONS {
        let parsed: Value = serde_json::from_str(&plan(question.text).to_json())?;
        write_json(&root.join(format!("plans/{}.json", question.id)), &parsed)?;
    }
    if plans_only {
        println!("{}", root.join("design.json").display());
        return Ok(());
    }

    write_json(&root.join("preflight.json"), &preflight_json())?;
    require_env("ALGOLIA_APPLICATION_ID_DOCS")?;
    require_env("ALGOLIA_API_KEY_DOCS")?;
    require_env("LUMENLOOP_API_KEY")?;

    let jev_dir = root.join("jev-client");
    let jev_config = run_config(&jev_dir, 1, 1, 60);
    let jev_http = HttpRecorder::new(&jev_dir, &jev_config)?;
    let client = JevClient::new(&jev_config, &jev_http)
        .context("Jev client was not created. No document was scored.")?;
    write_json(
        &root.join("usage.json"),
        &serde_json::to_value(client.usage())?,
    )?;

    let started = Instant::now();
    let mut fetch_rows = Vec::new();
    let mut admitted: BTreeMap<String, Admitted> = BTreeMap::new();
    let mut overflow = Vec::new();
    for spec in &schedule {
        if admitted.len() >= MAX_UNIQUE {
            fetch_rows.push(json!({
                "id": spec.id,
                "status": "not_started",
                "reason": "Unique document cap reached before this fetch."
            }));
            break;
        }
        let row = run_fetch(&root, spec, &registry).await?;
        let documents = row
            .get("documents")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for document in documents {
            let key = document["identity"].as_str().unwrap_or("").to_owned();
            if key.is_empty() {
                continue;
            }
            if let Some(existing) = admitted.get_mut(&key) {
                existing.observations.push(observation(spec, &document));
                existing.question_ids.insert(spec.question_id.clone());
            } else if admitted.len() >= MAX_UNIQUE {
                overflow.push(json!({
                    "identity": key,
                    "url": document["url"],
                    "fetch_id": spec.id,
                    "reason": "Unique document cap. Full fetch result still holds the text."
                }));
            } else {
                let path = save_document(&root, &document)?;
                admitted.insert(
                    key.clone(),
                    Admitted {
                        identity: key,
                        question_ids: BTreeSet::from([spec.question_id.clone()]),
                        source_id: document["source_id"].as_str().unwrap_or("").to_owned(),
                        url: document["url"].as_str().unwrap_or("").to_owned(),
                        title: document["title"].as_str().unwrap_or("").to_owned(),
                        sha256: document["text_sha256"].as_str().unwrap_or("").to_owned(),
                        bytes: document["text_bytes"].as_u64().unwrap_or(0),
                        path,
                        observations: vec![observation(spec, &document)],
                        document: decode_document(&document)?,
                    },
                );
            }
        }
        let summary = json!({
            "id": spec.id,
            "question_id": spec.question_id,
            "source_id": spec.source_id,
            "arm": spec.arm,
            "variant_kind": spec.variant_kind,
            "max_pages": spec.max_pages,
            "max_documents": spec.max_documents,
            "elapsed_ms": row["elapsed_ms"],
            "status": row["status"],
            "documents": row["document_count"],
            "failures": row["failures"],
            "search_requests": row["search_requests"],
            "result_path": row["result_path"]
        });
        println!("{summary}");
        fetch_rows.push(summary);
        write_progress(
            &root,
            &client,
            started,
            &fetch_rows,
            &admitted,
            &overflow,
            &[],
        )?;
    }

    let mut jobs = scoring_jobs(&admitted);
    let score_started = Instant::now();
    let mut scores = Vec::new();
    let mut score_stop = None;
    let mut cursor = 0usize;
    while !jobs.is_empty() {
        if client.usage().cost_usd >= SCORE_STOP_USD {
            score_stop = Some("Accounted cost reached the stop line under the $2 budget.");
            break;
        }
        if score_started.elapsed().as_secs() >= SCORE_WALL_SECS {
            score_stop =
                Some("Scoring wall clock reached 1500 seconds. Remaining documents stay unscored.");
            break;
        }
        let job = jobs.pop_front().unwrap();
        // Round-robin was applied when the queue was built.
        cursor += 1;
        let doc = admitted
            .get(&job.identity)
            .context("Missing admitted document")?;
        let one = if doc.document.text.trim().is_empty() {
            json!({
                "question_id": job.question_id,
                "original_question": job.original_question,
                "identity": job.identity,
                "source_id": doc.source_id,
                "url": doc.url,
                "status": "skipped",
                "error": "Document text is empty. Jev was not called."
            })
        } else {
            let t0 = Instant::now();
            match client
                .score_document(&job.original_question, &doc.document)
                .await
            {
                Ok(score) => json!({
                    "question_id": job.question_id,
                    "original_question": job.original_question,
                    "identity": job.identity,
                    "document_id": score.document_id,
                    "source_id": doc.source_id,
                    "url": doc.url,
                    "title": doc.title,
                    "text_sha256": doc.sha256,
                    "text_bytes": doc.bytes,
                    "path": doc.path,
                    "probability": score.probability,
                    "reason": score.reason,
                    "elapsed_ms": t0.elapsed().as_millis(),
                    "status": "scored",
                    "usage_after": serde_json::to_value(client.usage())?
                }),
                Err(error) => {
                    let message = error.to_string();
                    let stop = message.contains("budget") || message.contains("Jev stopped");
                    let row = json!({
                        "question_id": job.question_id,
                        "original_question": job.original_question,
                        "identity": job.identity,
                        "source_id": doc.source_id,
                        "url": doc.url,
                        "status": "error",
                        "error": message,
                        "elapsed_ms": t0.elapsed().as_millis(),
                        "usage_after": serde_json::to_value(client.usage())?
                    });
                    if stop {
                        scores.push(row);
                        score_stop =
                            Some("Jev refused another attempt. Later documents stay unscored.");
                        break;
                    }
                    row
                }
            }
        };
        println!(
            "{}",
            json!({
                "event": "score",
                "n": cursor,
                "status": one["status"],
                "question_id": job.question_id,
                "source_id": doc.source_id,
                "cost_usd": client.usage().cost_usd
            })
        );
        scores.push(one);
        write_json(&root.join("scores.json"), &json!(scores))?;
        write_json(
            &root.join("usage.json"),
            &serde_json::to_value(client.usage())?,
        )?;
    }
    let unscored: Vec<_> = jobs
        .iter()
        .map(|job| {
            let doc = admitted.get(&job.identity);
            json!({
                "question_id": job.question_id,
                "identity": job.identity,
                "source_id": doc.map(|d| d.source_id.as_str()).unwrap_or(""),
                "url": doc.map(|d| d.url.as_str()).unwrap_or(""),
                "reason": score_stop.unwrap_or("not started")
            })
        })
        .collect();
    write_json(&root.join("unscored.json"), &json!(unscored))?;
    let overlap = overlap_report(&admitted);
    write_json(&root.join("overlap.json"), &overlap)?;
    write_json(
        &root.join("review-queue.json"),
        &review_queue(&admitted, &scores),
    )?;
    write_progress(
        &root,
        &client,
        started,
        &fetch_rows,
        &admitted,
        &overflow,
        &scores,
    )?;
    write_json(
        &root.join("manifest.json"),
        &json!({
            "status": if score_stop.is_none() && overflow.is_empty() { "complete" } else { "stopped" },
            "score_stop": score_stop,
            "elapsed_ms": started.elapsed().as_millis(),
            "source_fetches_started": fetch_rows.iter().filter(|row| row["status"] != "not_started").count(),
            "unique_documents": admitted.len(),
            "scores": scores.iter().filter(|row| row["status"] == "scored").count(),
            "score_errors": scores.iter().filter(|row| row["status"] == "error").count(),
            "unscored": unscored.len(),
            "overflow": overflow.len(),
            "usage": serde_json::to_value(client.usage())?,
            "quality_claim": "This file does not judge relevance. Jev probabilities are uncalibrated scores."
        }),
    )?;
    println!("{}", root.display());
    Ok(())
}

struct Admitted {
    identity: String,
    question_ids: BTreeSet<String>,
    source_id: String,
    url: String,
    title: String,
    sha256: String,
    bytes: u64,
    path: String,
    observations: Vec<Value>,
    document: Document,
}

struct ScoreJob {
    question_id: String,
    original_question: String,
    identity: String,
}

fn build_schedule(registry: &[Source]) -> Result<(Vec<FetchSpec>, Vec<Value>)> {
    let ids: BTreeSet<_> = registry.iter().map(|source| source.id.as_str()).collect();
    let mut schedule = Vec::new();
    for question in QUESTIONS {
        for source_id in question
            .equal_sources
            .iter()
            .copied()
            .chain([question.focused_source, question.variant_source])
        {
            anyhow::ensure!(ids.contains(source_id), "Unknown source id: {source_id}");
        }
        anyhow::ensure!(
            question.focused_pages <= MAX_PAGES,
            "Focused pages exceed 3"
        );
        for source_id in question.equal_sources {
            schedule.push(spec(
                question,
                "equal_depth",
                source_id,
                question.text,
                "natural",
                EQUAL_PAGES,
                EQUAL_DOCS,
            ));
        }
        schedule.push(spec(
            question,
            "focused_depth",
            question.focused_source,
            question.text,
            "natural",
            question.focused_pages,
            question.focused_documents,
        ));
    }
    let mut extras = Vec::new();
    for question in QUESTIONS {
        extras.extend(variants_for(question, VariantKind::Facet));
    }
    for question in QUESTIONS {
        extras.extend(variants_for(question, VariantKind::Keywords));
    }
    let room = MAX_FETCHES.saturating_sub(schedule.len());
    let mut dropped = Vec::new();
    for extra in extras.into_iter().skip(room) {
        dropped.push(json!({
            "question_id": extra.question_id,
            "source_id": extra.source_id,
            "variant_kind": extra.variant_kind,
            "fetch_question": extra.fetch_question,
            "reason": "The 20 source-fetch cap omitted this variant."
        }));
    }
    let mut kept = Vec::new();
    // Rebuild kept extras without consuming dropped texts twice.
    let mut extras = Vec::new();
    for question in QUESTIONS {
        extras.extend(variants_for(question, VariantKind::Facet));
    }
    for question in QUESTIONS {
        extras.extend(variants_for(question, VariantKind::Keywords));
    }
    for extra in extras.into_iter().take(room) {
        kept.push(extra);
    }
    schedule.extend(kept);
    for (index, item) in schedule.iter_mut().enumerate() {
        item.id = format!("{:02}", index + 1);
    }
    Ok((schedule, dropped))
}

fn variants_for(question: &QuestionDef, kind: VariantKind) -> Vec<FetchSpec> {
    let planned = plan(question.text);
    let mut rows = Vec::new();
    let variants = match kind {
        VariantKind::Facet => planned.semantic(),
        VariantKind::Keywords => planned.keyword(),
        VariantKind::Natural | VariantKind::Entity => return rows,
    };
    for variant in variants {
        if variant.kind != kind {
            continue;
        }
        if variant.text.eq_ignore_ascii_case(question.text) {
            continue;
        }
        if question.variant_source == "algolia:docs:primary"
            && variant.text == planned.keyword_text()
        {
            continue;
        }
        rows.push(spec(
            question,
            "query_variant",
            question.variant_source,
            &variant.text,
            variant.kind.as_str(),
            VARIANT_PAGES,
            VARIANT_DOCS,
        ));
    }
    rows
}

fn spec(
    question: &QuestionDef,
    arm: &str,
    source_id: &str,
    fetch_question: &str,
    variant_kind: &str,
    max_pages: usize,
    max_documents: usize,
) -> FetchSpec {
    FetchSpec {
        id: String::new(),
        question_id: question.id.into(),
        original_question: question.text.into(),
        source_id: source_id.into(),
        arm: arm.into(),
        fetch_question: fetch_question.into(),
        variant_kind: variant_kind.into(),
        max_pages,
        max_documents,
    }
}

fn spec_json(spec: &FetchSpec) -> Value {
    json!({
        "id": spec.id,
        "question_id": spec.question_id,
        "original_question": spec.original_question,
        "source_id": spec.source_id,
        "arm": spec.arm,
        "fetch_question": spec.fetch_question,
        "variant_kind": spec.variant_kind,
        "max_pages": spec.max_pages,
        "max_documents": spec.max_documents
    })
}

async fn run_fetch(root: &Path, spec: &FetchSpec, registry: &[Source]) -> Result<Value> {
    let dir = root.join("fetches").join(&spec.id);
    std::fs::create_dir_all(&dir)?;
    write_json(&dir.join("request.json"), &spec_json(spec))?;
    let source = registry
        .iter()
        .find(|source| source.id == spec.source_id)
        .context("Source missing at fetch time")?
        .clone();
    let config = run_config(&dir, spec.max_pages, spec.max_documents, 30);
    write_json(
        &dir.join("run-config.json"),
        &serde_json::to_value(&config)?,
    )?;
    let started = Instant::now();
    let ctx = FetchContext {
        http: HttpRecorder::new(&dir, &config)?,
        config,
    };
    let outcome = match tokio::time::timeout(
        std::time::Duration::from_secs(FETCH_TIMEOUT_SECS),
        connectors::fetch(&ctx, &source, &spec.fetch_question),
    )
    .await
    {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(error)) => Err(error),
        Err(_) => anyhow::bail!("Source fetch timed out after {FETCH_TIMEOUT_SECS} seconds"),
    };
    let elapsed_ms = started.elapsed().as_millis();
    let (status, result_value, failures) = match outcome {
        Ok(result) => {
            let value = serde_json::to_value(&result)?;
            write_json(&dir.join("result.json"), &value)?;
            ("ok", value, result.failures)
        }
        Err(error) => {
            let failures = vec![stellar_raven_jev::types::Failure {
                stage: "query_probe".into(),
                source_id: Some(spec.source_id.clone()),
                message: error.to_string(),
            }];
            let value = json!({"documents": [], "failures": failures});
            write_json(&dir.join("result.json"), &value)?;
            ("error", value, failures)
        }
    };
    let observed = observed_requests(&dir)?;
    write_json(&dir.join("observed-requests.json"), &observed)?;
    let search_requests = observed
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter(|row| {
                    let url = row["url"].as_str().unwrap_or("");
                    url.contains("/query")
                        || url.contains("/api/research")
                        || url.contains("search_content_semantic")
                })
                .count()
        })
        .unwrap_or(0);
    let documents = result_value["documents"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|document| index_document(spec, document))
        .collect::<Result<Vec<_>>>()?;
    write_json(
        &dir.join("timing.json"),
        &json!({"elapsed_ms": elapsed_ms, "status": status}),
    )?;
    Ok(json!({
        "status": status,
        "elapsed_ms": elapsed_ms,
        "document_count": documents.len(),
        "documents": documents,
        "failures": failures,
        "search_requests": search_requests,
        "result_path": format!("fetches/{}/result.json", spec.id)
    }))
}

fn index_document(spec: &FetchSpec, document: Value) -> Result<Value> {
    let text = document["text"].as_str().unwrap_or("");
    let sha = sha256_hex(text.as_bytes())?;
    let url = document["url"].as_str().unwrap_or("").to_owned();
    let source_id = document["source_id"]
        .as_str()
        .unwrap_or(&spec.source_id)
        .to_owned();
    Ok(json!({
        "identity": format!("{source_id}\n{url}\n{sha}"),
        "source_id": source_id,
        "url": url,
        "title": document["title"],
        "text": text,
        "text_sha256": sha,
        "text_bytes": text.len(),
        "provenance": document["provenance"],
        "raw_artifacts": document["raw_artifacts"],
        "connector_id": document["id"],
        "question_id": spec.question_id,
        "arm": spec.arm,
        "fetch_id": spec.id
    }))
}

fn observation(spec: &FetchSpec, document: &Value) -> Value {
    json!({
        "fetch_id": spec.id,
        "arm": spec.arm,
        "question_id": spec.question_id,
        "variant_kind": spec.variant_kind,
        "fetch_question": spec.fetch_question,
        "original_question": spec.original_question,
        "connector_id": document["connector_id"],
        "raw_artifacts": document["raw_artifacts"]
    })
}

fn save_document(root: &Path, document: &Value) -> Result<String> {
    let sha = document["text_sha256"].as_str().unwrap_or("none");
    let source = document["source_id"]
        .as_str()
        .unwrap_or("source")
        .replace([':', '/'], "-");
    let relative = format!("documents/{sha}-{source}.md");
    let path = root.join(&relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::File::create(&path)?;
    writeln!(
        file,
        "# {}",
        document["title"].as_str().unwrap_or("Untitled")
    )?;
    writeln!(file)?;
    writeln!(
        file,
        "Source: {}",
        document["source_id"].as_str().unwrap_or("")
    )?;
    writeln!(file, "URL: {}", document["url"].as_str().unwrap_or(""))?;
    writeln!(file, "SHA-256: {sha}")?;
    writeln!(
        file,
        "Bytes: {}",
        document["text_bytes"].as_u64().unwrap_or(0)
    )?;
    writeln!(file)?;
    writeln!(file, "{}", document["text"].as_str().unwrap_or(""))?;
    Ok(relative)
}

fn decode_document(document: &Value) -> Result<Document> {
    Ok(Document {
        id: document["connector_id"]
            .as_str()
            .unwrap_or("document")
            .to_owned(),
        source_id: document["source_id"].as_str().unwrap_or("").to_owned(),
        title: document["title"].as_str().unwrap_or("").to_owned(),
        url: document["url"].as_str().unwrap_or("").to_owned(),
        text: document["text"].as_str().unwrap_or("").to_owned(),
        provenance: document["provenance"].clone(),
        raw_artifacts: document["raw_artifacts"]
            .as_array()
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default(),
    })
}

fn scoring_jobs(admitted: &BTreeMap<String, Admitted>) -> VecDeque<ScoreJob> {
    let mut by_question: BTreeMap<&str, Vec<ScoreJob>> = BTreeMap::new();
    for question in QUESTIONS {
        by_question.insert(question.id, Vec::new());
    }
    for doc in admitted.values() {
        for question in QUESTIONS {
            if doc.question_ids.contains(question.id) {
                by_question.get_mut(question.id).unwrap().push(ScoreJob {
                    question_id: question.id.into(),
                    original_question: question.text.into(),
                    identity: doc.identity.clone(),
                });
            }
        }
    }
    let mut queues: Vec<VecDeque<ScoreJob>> = QUESTIONS
        .iter()
        .map(|question| {
            let mut jobs = by_question.remove(question.id).unwrap_or_default();
            jobs.sort_by_key(|job| {
                admitted
                    .get(&job.identity)
                    .map(|doc| doc.bytes)
                    .unwrap_or(u64::MAX)
            });
            jobs.into()
        })
        .collect();
    let mut jobs = VecDeque::new();
    loop {
        let mut added = false;
        for queue in &mut queues {
            if let Some(job) = queue.pop_front() {
                jobs.push_back(job);
                added = true;
            }
        }
        if !added {
            break;
        }
    }
    jobs
}

fn overlap_report(admitted: &BTreeMap<String, Admitted>) -> Value {
    let mut questions = Vec::new();
    for question in QUESTIONS {
        let docs: Vec<_> = admitted
            .values()
            .filter(|doc| doc.question_ids.contains(question.id))
            .collect();
        let urls = |arm: &str| -> BTreeSet<String> {
            docs.iter()
                .filter(|doc| {
                    doc.observations
                        .iter()
                        .any(|row| row["question_id"] == question.id && row["arm"] == arm)
                })
                .map(|doc| doc.url.clone())
                .filter(|url| !url.is_empty())
                .collect()
        };
        let equal = urls("equal_depth");
        let focused = urls("focused_depth");
        let variant = urls("query_variant");
        let both_depth: Vec<_> = equal.intersection(&focused).cloned().collect();
        let focused_only: Vec<_> = focused.difference(&equal).cloned().collect();
        let equal_only: Vec<_> = equal.difference(&focused).cloned().collect();
        let prose_source_urls: BTreeSet<_> = docs
            .iter()
            .filter(|doc| {
                doc.observations.iter().any(|row| {
                    row["question_id"] == question.id
                        && row["arm"] == "equal_depth"
                        && row_source_matches(doc, question.variant_source)
                })
            })
            .map(|doc| doc.url.clone())
            .filter(|url| !url.is_empty())
            .collect();
        let variant_only: Vec<_> = variant.difference(&prose_source_urls).cloned().collect();
        let variant_overlap: Vec<_> = variant.intersection(&prose_source_urls).cloned().collect();
        questions.push(json!({
            "question_id": question.id,
            "unique_documents": docs.len(),
            "equal_urls": equal,
            "focused_urls": focused,
            "variant_urls": variant,
            "depth_url_overlap": both_depth,
            "focused_urls_not_in_equal": focused_only,
            "equal_urls_not_in_focused": equal_only,
            "variant_source": question.variant_source,
            "variant_url_overlap_with_equal_same_source": variant_overlap,
            "variant_urls_not_in_equal_same_source": variant_only
        }));
    }
    json!({
        "url_identity": "Document.url as returned. Empty URLs are omitted from URL sets.",
        "questions": questions
    })
}

fn row_source_matches(doc: &Admitted, source_id: &str) -> bool {
    doc.source_id == source_id
}

fn review_queue(admitted: &BTreeMap<String, Admitted>, scores: &[Value]) -> Value {
    let mut rows = Vec::new();
    for question in QUESTIONS {
        let mut ranked: Vec<&Value> = scores
            .iter()
            .filter(|row| row["question_id"] == question.id && row["status"] == "scored")
            .collect();
        ranked.sort_by(|a, b| {
            let left = a["probability"].as_f64().unwrap_or(0.0);
            let right = b["probability"].as_f64().unwrap_or(0.0);
            right
                .partial_cmp(&left)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        if let Some(top) = ranked.first() {
            rows.push(queue_row(question.id, "highest_jev_score", top, admitted));
        }
        if let Some(low) = ranked.last() {
            if ranked.len() > 1 {
                rows.push(queue_row(question.id, "lowest_jev_score", low, admitted));
            }
        }
        for (label, arm) in [
            ("equal_observation", "equal_depth"),
            ("focused_observation", "focused_depth"),
            ("variant_observation", "query_variant"),
        ] {
            if let Some(doc) = admitted.values().find(|doc| {
                doc.question_ids.contains(question.id)
                    && doc
                        .observations
                        .iter()
                        .any(|row| row["question_id"] == question.id && row["arm"] == arm)
            }) {
                rows.push(json!({
                    "question_id": question.id,
                    "reason": label,
                    "path": doc.path,
                    "url": doc.url,
                    "source_id": doc.source_id,
                    "title": doc.title,
                    "text_bytes": doc.bytes,
                    "identity": doc.identity
                }));
            }
        }
    }
    json!({
        "purpose": "Paths for an independent full-text read. This queue is not a relevance judgment.",
        "items": rows
    })
}

fn queue_row(
    question_id: &str,
    reason: &str,
    score: &Value,
    admitted: &BTreeMap<String, Admitted>,
) -> Value {
    let identity = score["identity"].as_str().unwrap_or("");
    let doc = admitted.get(identity);
    json!({
        "question_id": question_id,
        "reason": reason,
        "path": doc.map(|d| d.path.as_str()).unwrap_or(""),
        "url": score["url"],
        "source_id": score["source_id"],
        "title": score["title"],
        "probability": score["probability"],
        "text_bytes": score["text_bytes"],
        "identity": identity
    })
}

fn observed_requests(dir: &Path) -> Result<Value> {
    let mut paths = Vec::new();
    let raw = dir.join("raw");
    if raw.is_dir() {
        for entry in std::fs::read_dir(raw)? {
            let path = entry?.path();
            if path.extension().is_some_and(|ext| ext == "json") {
                paths.push(path);
            }
        }
    }
    paths.sort();
    let mut rows = Vec::new();
    for path in paths {
        let value: Value = serde_json::from_str(&std::fs::read_to_string(&path)?)?;
        let url = value["url"].as_str().unwrap_or("").to_owned();
        let mut query_pairs = serde_json::Map::new();
        if let Ok(parsed) = reqwest::Url::parse(&url) {
            for (key, item) in parsed.query_pairs() {
                if matches!(key.as_ref(), "q" | "source" | "limit" | "offset" | "page") {
                    query_pairs.insert(key.into_owned(), json!(item));
                }
            }
        }
        rows.push(json!({
            "file": path.file_name().and_then(|name| name.to_str()),
            "method": value["method"],
            "url": url,
            "status": value["status"],
            "complete": value["complete"],
            "query": value["request_body"].get("query").cloned().unwrap_or(Value::Null),
            "page": value["request_body"].get("page").cloned().unwrap_or(Value::Null),
            "hitsPerPage": value["request_body"].get("hitsPerPage").cloned().unwrap_or(Value::Null),
            "types": value["request_body"].get("types").cloned().unwrap_or(Value::Null),
            "limit": value["request_body"].get("limit").cloned().unwrap_or(Value::Null),
            "url_params": query_pairs,
            "failure": value.get("failure").cloned().unwrap_or(Value::Null)
        }));
    }
    Ok(json!(rows))
}

fn write_progress(
    root: &Path,
    client: &JevClient,
    started: Instant,
    fetches: &[Value],
    admitted: &BTreeMap<String, Admitted>,
    overflow: &[Value],
    scores: &[Value],
) -> Result<()> {
    let documents: Vec<_> = admitted
        .values()
        .map(|doc| {
            json!({
                "identity": doc.identity,
                "question_ids": doc.question_ids,
                "source_id": doc.source_id,
                "url": doc.url,
                "title": doc.title,
                "text_sha256": doc.sha256,
                "text_bytes": doc.bytes,
                "path": doc.path,
                "observations": doc.observations
            })
        })
        .collect();
    write_json(&root.join("documents.json"), &json!(documents))?;
    write_json(&root.join("fetches.json"), &json!(fetches))?;
    write_json(&root.join("overflow.json"), &json!(overflow))?;
    write_json(
        &root.join("usage.json"),
        &serde_json::to_value(client.usage())?,
    )?;
    write_json(
        &root.join("timing.json"),
        &json!({"elapsed_ms": started.elapsed().as_millis(), "fetches": fetches.len(), "unique_documents": admitted.len(), "scores": scores.len()}),
    )?;
    Ok(())
}

fn run_config(dir: &Path, max_pages: usize, max_documents: usize, timeout_secs: u64) -> RunConfig {
    RunConfig {
        fixture: false,
        output_dir: dir.to_path_buf(),
        budget_usd: BUDGET_USD,
        timeout_secs,
        concurrency: 2,
        max_pages,
        max_documents,
        per_source_documents: max_documents,
        fetch_deadline_secs: 10,
        max_body_bytes: 8 * 1024 * 1024,
        route_passes: 0,
        source_threshold: 0.2,
        document_threshold: 0.4,
        uncertain_threshold: 0.15,
    }
}

fn preflight_json() -> Value {
    json!({
        "env_present": {
            "ALGOLIA_APPLICATION_ID_DOCS": env_present("ALGOLIA_APPLICATION_ID_DOCS"),
            "ALGOLIA_API_KEY_DOCS": env_present("ALGOLIA_API_KEY_DOCS"),
            "LUMENLOOP_API_KEY": env_present("LUMENLOOP_API_KEY"),
            "JEV_BACKEND": env_present("JEV_BACKEND"),
            "CLOUDFLARE_ACCOUNT_ID": env_present("CLOUDFLARE_ACCOUNT_ID"),
            "CLOUDFLARE_API_TOKEN": env_present("CLOUDFLARE_API_TOKEN"),
            "JEV_CLOUDFLARE_AUTH_PROFILE": env_present("JEV_CLOUDFLARE_AUTH_PROFILE")
        },
        "values_printed": false
    })
}

fn env_present(name: &str) -> bool {
    std::env::var_os(name).is_some_and(|value| !value.is_empty())
}

fn require_env(name: &str) -> Result<()> {
    anyhow::ensure!(
        env_present(name),
        "{name} is missing. No live call was made."
    );
    Ok(())
}

fn write_json(path: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> Result<String> {
    let mut child = std::process::Command::new("shasum")
        .args(["-a", "256"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .context("Cannot start shasum")?;
    child
        .stdin
        .take()
        .context("shasum stdin")?
        .write_all(bytes)?;
    let output = child.wait_with_output()?;
    anyhow::ensure!(output.status.success(), "shasum failed");
    let text = String::from_utf8(output.stdout)?;
    let hash = text.split_whitespace().next().unwrap_or("");
    anyhow::ensure!(hash.len() == 64, "shasum digest length");
    Ok(hash.to_ascii_lowercase())
}
