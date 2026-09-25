//! Sessions: a run folder that later calls extend. `search` starts one; `more` spends its pools
//! (see `pipeline::continue_session`); `check` asks Jev whether the session's documents support,
//! contradict, or qualify claims the agent wrote. Every call reports the session's cumulative
//! usage.

use crate::{
    http::HttpRecorder,
    jev::JevClient,
    types::{Document, DocumentScore, RunConfig, Usage},
};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

fn read<T: serde::de::DeserializeOwned>(root: &Path, name: &str) -> Result<T> {
    serde_json::from_slice(&std::fs::read(root.join(name))?)
        .with_context(|| format!("{name} in the session is unreadable"))
}

fn write(root: &Path, name: &str, value: &Value) -> Result<()> {
    std::fs::write(root.join(name), serde_json::to_vec(value)?)?;
    Ok(())
}

/// Record one call in `session.json`: the command, its arguments, and the session usage after it.
pub fn record_call(root: &Path, command: &str, arguments: Value, usage: &Usage) -> Result<()> {
    let mut session: Value = read(root, "session.json").unwrap_or_else(|_| json!({"calls":[]}));
    // Source requests this call sent; `check` sends none.
    let load: Value = if command == "check" {
        Value::Null
    } else {
        read(root, "load.json").unwrap_or(Value::Null)
    };
    let calls = session["calls"]
        .as_array_mut()
        .context("session.json has no call list")?;
    calls.push(json!({
        "command": command,
        "arguments": arguments,
        "usage_after": usage,
        "source_requests": load["source_requests"],
        "unix_ms": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
    }));
    write(root, "session.json", &session)
}

/// Fix the session's spending cap at its first call: three times that call's `--budget-usd`.
pub fn set_budget_cap(root: &Path, budget_usd: f64) -> Result<()> {
    let mut session: Value = read(root, "session.json")?;
    if session["budget_cap_usd"].is_null() {
        session["budget_cap_usd"] = json!(budget_usd * crate::pipeline::SESSION_BUDGET_MULTIPLE);
        write(root, "session.json", &session)?;
    }
    Ok(())
}

/// The session's spending cap, fixed at its first call.
pub fn budget_cap(root: &Path) -> Result<f64> {
    read::<Value>(root, "session.json")?["budget_cap_usd"]
        .as_f64()
        .context("session.json lacks the session budget cap")
}

/// Mark a paid call as started, with the most it may spend. A call that ends normally clears the
/// mark with `end_call`. A mark left by a call that stopped counts its whole allowance as spent,
/// so an interrupted call can never let the session spend past its cap.
pub fn begin_call(root: &Path, allowance_usd: f64) -> Result<()> {
    settle_stopped_call(root)?;
    let mut session: Value = read(root, "session.json")?;
    session["in_progress_usd"] = json!(allowance_usd);
    write(root, "session.json", &session)
}

/// Charge the whole allowance of a call that stopped before it saved its usage. Run it before a
/// new call checks the cap.
pub fn settle_stopped_call(root: &Path) -> Result<()> {
    let mut session: Value = read(root, "session.json")?;
    let Some(unsettled) = session["in_progress_usd"].as_f64() else {
        return Ok(());
    };
    let mut usage: Usage = read(root, "usage.json")?;
    usage.cost_usd += unsettled;
    write(root, "usage.json", &json!(usage))?;
    session["unsettled_usd"] = json!(session["unsettled_usd"].as_f64().unwrap_or(0.0) + unsettled);
    if let Some(object) = session.as_object_mut() {
        object.remove("in_progress_usd");
    }
    write(root, "session.json", &session)
}

/// Clear the mark of a call that ended normally and saved its usage.
pub fn end_call(root: &Path) -> Result<()> {
    let mut session: Value = read(root, "session.json")?;
    if let Some(object) = session.as_object_mut() {
        object.remove("in_progress_usd");
    }
    write(root, "session.json", &session)
}

/// Hold the session for one call. `None` when another call holds it; the caller reports busy.
/// The lock is a file lock, so a process that dies releases it.
pub fn lock(root: &Path) -> Result<Option<std::fs::File>> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join("session.lock"))
        .context("Cannot open the session lock")?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(std::fs::TryLockError::Error(error)) => Err(error).context("Cannot lock the session"),
    }
}

/// The session summary every call prints: its folder, the calls so far, cumulative usage, and how
/// many documents are scored and still unscored.
pub fn session_view(root: &Path, usage: &Usage) -> Value {
    let calls = read::<Value>(root, "session.json")
        .ok()
        .and_then(|s| s["calls"].as_array().map(Vec::len))
        .unwrap_or(0);
    let scored = read::<Vec<Value>>(root, "scores.json").map_or(0, |s| s.len());
    let unscored = read::<Vec<Value>>(root, "deferred.json").map_or(0, |s| s.len());
    let mut requests: BTreeMap<String, u64> = BTreeMap::new();
    if let Ok(session) = read::<Value>(root, "session.json") {
        for call in session["calls"].as_array().into_iter().flatten() {
            for (host, n) in call["source_requests"].as_object().into_iter().flatten() {
                *requests.entry(host.clone()).or_default() += n.as_u64().unwrap_or(0);
            }
        }
    }
    json!({
        "id": root,
        "calls": calls,
        "usage": usage,
        "source_requests": requests,
        "documents_scored": scored,
        "documents_unscored": unscored,
    })
}

/// The session's open pools, facts only: each source's own name and description, its routing
/// probability, its state (`unscored_tail`: fetched documents not yet scored; `unfetched`: routed
/// but not fetched), what is pending, what the session already scored and selected from it, and
/// whether spending it sends a source request. Ordered by routing probability.
pub fn pools_view(root: &Path) -> Result<Value> {
    let decisions: Vec<Value> = read(root, "source-decisions.json")?;
    let deferred: Vec<Document> = read(root, "deferred.json")?;
    let documents: Vec<Document> = read(root, "documents.json")?;
    let scores: Vec<DocumentScore> = read(root, "scores.json")?;
    let classification: Value = read(root, "classification.json")?;
    let selected: BTreeSet<&str> = classification["selected"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    let source_of: BTreeMap<&str, &str> = documents
        .iter()
        .map(|d| (d.id.as_str(), d.source_id.as_str()))
        .collect();
    let mut stats: BTreeMap<&str, (usize, usize, f64)> = BTreeMap::new();
    for score in &scores {
        if let Some(source) = source_of.get(score.document_id.as_str()) {
            let entry = stats.entry(source).or_insert((0, 0, 0.0));
            entry.0 += 1;
            entry.1 += usize::from(selected.contains(score.document_id.as_str()));
            entry.2 = entry.2.max(score.probability);
        }
    }
    let mut tails: BTreeMap<&str, usize> = BTreeMap::new();
    for document in &deferred {
        *tails.entry(document.source_id.as_str()).or_default() += 1;
    }
    let registry = crate::connectors::sources();
    let mut pools = Vec::new();
    for decision in &decisions {
        let Some(id) = decision["source_id"].as_str() else {
            continue;
        };
        let state = if decision["selected"] == true && decision["fetched"] == false {
            "unfetched"
        } else if tails.contains_key(id) {
            "unscored_tail"
        } else {
            continue;
        };
        let source = registry.iter().find(|s| s.id == id);
        let (scored, chosen, best) = stats.get(id).copied().unwrap_or((0, 0, 0.0));
        pools.push(json!({
            "id": id,
            "name": source.map(|s| s.name.as_str()),
            "description": source.map(|s| s.description.as_str()),
            "route": decision["max_probability"],
            "state": state,
            "pending": tails.get(id),
            "scored": scored,
            "selected": chosen,
            "best": (scored > 0).then_some(best),
            "source_requests": if state == "unfetched" { 1 } else { 0 },
        }));
    }
    pools.sort_by(|a, b| {
        b["route"]
            .as_f64()
            .unwrap_or(0.0)
            .total_cmp(&a["route"].as_f64().unwrap_or(0.0))
    });
    Ok(Value::Array(pools))
}

/// The compact view of the pools: counts, the best routing and tail probabilities, and only the
/// pools with a signal an agent can act on: an unscored tail whose best scored document reached
/// the uncertain threshold, or an unfetched source routed within 0.1 of the fetch threshold. The
/// full list stays in the full report (`--json`).
pub fn pool_summary(pools: &Value, config: &Value) -> Value {
    let rows = pools.as_array().cloned().unwrap_or_default();
    let uncertain = config["uncertain_threshold"].as_f64().unwrap_or(0.15);
    let fetch = config["fetch_threshold"].as_f64().unwrap_or(0.2);
    let of = |state: &'static str| rows.iter().filter(move |p| p["state"] == state);
    let max = |values: Vec<f64>| values.into_iter().reduce(f64::max);
    let actionable: Vec<Value> = rows
        .iter()
        .filter(|p| match p["state"].as_str() {
            Some("unscored_tail") => p["best"].as_f64().unwrap_or(0.0) >= uncertain,
            Some("unfetched") => p["route"].as_f64().unwrap_or(0.0) >= fetch - 0.1,
            _ => false,
        })
        .cloned()
        .collect();
    json!({
        "unfetched": of("unfetched").count(),
        "unscored_tails": of("unscored_tail").count(),
        "best_unfetched_route": max(of("unfetched").filter_map(|p| p["route"].as_f64()).collect()),
        "best_tail_score": max(of("unscored_tail").filter_map(|p| p["best"].as_f64()).collect()),
        "actionable": actionable,
    })
}

/// Which documents a claim check reads.
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum CheckScope {
    /// Selected and uncertain documents.
    Selected,
    /// Every scored document.
    Scored,
    /// Every scored document and the unscored ones the session holds.
    All,
}

/// The text file for a session document: its position in documents.json, or, for a document not
/// yet scored, a name from its ID. Written when missing.
fn text_file(root: &Path, position: Option<usize>, document: &Document) -> Result<PathBuf> {
    let dir = root.join("search-documents");
    std::fs::create_dir_all(&dir)?;
    let path = match position {
        Some(p) => dir.join(format!("{p:04}.txt")),
        None => {
            use sha2::{Digest, Sha256};
            let digest = format!("{:x}", Sha256::digest(document.id.as_bytes()));
            dir.join(format!("unscored-{}.txt", &digest[..12]))
        }
    };
    if !path.exists() {
        std::fs::write(&path, &document.text)?;
    }
    Ok(path)
}

/// Ask Jev, for each claim, which session documents support it, contradict it, or qualify it.
/// Rows at 0.5 or above are listed, strongest first, `limit` per list; the full judgment of every
/// document is saved under `checks/`. There is no verdict: the agent reads and decides.
pub async fn check(
    root: &Path,
    claims: &[String],
    scope: CheckScope,
    limit: usize,
    current: &RunConfig,
) -> Result<Value> {
    ensure!(!claims.is_empty(), "Give at least one claim");
    ensure!(claims.len() <= 4, "Check at most four claims at once");
    ensure!(
        claims
            .iter()
            .all(|c| !c.trim().is_empty() && c.len() <= 1_000),
        "Each claim must be nonempty and at most 1,000 bytes"
    );
    let root = root
        .canonicalize()
        .context("The session folder does not exist")?;
    let record: Value = read(&root, "question.json")?;
    let question = record["question"]
        .as_str()
        .context("question.json lacks the question")?;
    let mut config: RunConfig = serde_json::from_value(record["config"].clone())
        .context("question.json has unreadable settings")?;
    config.output_dir = root.clone();
    config.host_dir = current.host_dir.clone();
    settle_stopped_call(&root)?;
    let prior: Usage = read(&root, "usage.json")?;
    let cap = budget_cap(&root)?;
    ensure!(
        config.fixture || cap - prior.cost_usd > 0.001,
        "The session has spent its budget of ${cap:.3}"
    );
    config.budget_usd = current.budget_usd.min(cap - prior.cost_usd);
    begin_call(&root, config.budget_usd)?;
    let documents: Vec<Document> = read(&root, "documents.json")?;
    let with_score: BTreeSet<String> = read::<Vec<DocumentScore>>(&root, "scores.json")?
        .into_iter()
        .map(|s| s.document_id)
        .collect();
    let classification: Value = read(&root, "classification.json")?;
    let status_of: BTreeMap<String, &str> = ["selected", "uncertain", "rejected"]
        .iter()
        .flat_map(|status| {
            classification[*status]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(move |id| (id.to_owned(), *status))
        })
        .collect();
    let mut chosen: Vec<(Option<usize>, Document, &str)> = documents
        .iter()
        .enumerate()
        .filter_map(|(i, d)| {
            let status = status_of.get(&d.id).copied().unwrap_or("uncertain");
            let keep = match scope {
                CheckScope::Selected => matches!(status, "selected" | "uncertain"),
                CheckScope::Scored => with_score.contains(&d.id),
                CheckScope::All => true,
            };
            keep.then(|| (Some(i + 1), d.clone(), status))
        })
        .collect();
    if matches!(scope, CheckScope::All) {
        for d in read::<Vec<Document>>(&root, "deferred.json")? {
            chosen.push((None, d, "unscored"));
        }
    }
    let http = HttpRecorder::resume(&root, &config)?;
    let jev = JevClient::new(&config, &http.with_concurrency(config.jev_concurrency))?;
    let docs: Vec<Document> = chosen.iter().map(|(_, d, _)| d.clone()).collect();
    let judged = jev.judge_claims(question, claims, &docs).await;
    let usage = crate::pipeline::add_usage(&prior, &jev.usage());
    std::fs::write(root.join("usage.json"), serde_json::to_vec(&usage)?)?;
    end_call(&root)?;
    // `report` reads the manifest's outcome, so it carries the session's usage too.
    let mut manifest: Value = read(&root, "manifest.json")?;
    manifest["outcome"]["usage"] = json!(usage);
    write(&root, "manifest.json", &manifest)?;
    let today = crate::rank::iso_date_days(&config.today).unwrap_or_default();
    let mut failed = 0;
    let mut full = Vec::new();
    let mut per_claim: Vec<Vec<Value>> = claims.iter().map(|_| Vec::new()).collect();
    for ((position, document, status), result) in chosen.iter().zip(judged) {
        let judgments = match result {
            Ok(j) => j,
            Err(_) => {
                failed += 1;
                continue;
            }
        };
        let scope_name = crate::search::content_scope(document);
        let text_path = text_file(&root, *position, document)?;
        for (j, judgment) in judgments.iter().enumerate() {
            // Each list shows the chunk that gave its own probability.
            let excerpts: Vec<String> = judgment
                .chunks
                .iter()
                .map(|[start, end]| {
                    document
                        .text
                        .get(*start..*end)
                        .unwrap_or(&document.text)
                        .chars()
                        .take(400)
                        .collect()
                })
                .collect();
            let row = json!({
                "title": document.title, "url": document.url, "source_id": document.source_id,
                "status": status, "supports": judgment.supports, "contradicts": judgment.contradicts,
                "qualifies": judgment.qualifies,
                "date": crate::rank::document_date(document, today),
                "authority_tier": crate::rank::authority_tier(document, scope_name),
                "content_scope": scope_name, "excerpts": excerpts, "text_path": text_path,
            });
            full.push(json!({"claim": j, "document_id": document.id, "judgment": judgment}));
            per_claim[j].push(row);
        }
    }
    let lists = |rows: &[Value], signal: &str| -> Vec<Value> {
        let witness = match signal {
            "supports" => 0,
            "contradicts" => 1,
            _ => 2,
        };
        let mut hits: Vec<Value> = rows
            .iter()
            .filter(|r| r[signal].as_f64().unwrap_or(0.0) >= 0.5)
            .map(|r| {
                let mut row = r.clone();
                row["excerpt"] = r["excerpts"][witness].clone();
                row.as_object_mut().map(|o| o.remove("excerpts"));
                row
            })
            .collect();
        hits.sort_by(|a, b| {
            b[signal]
                .as_f64()
                .unwrap_or(0.0)
                .total_cmp(&a[signal].as_f64().unwrap_or(0.0))
        });
        hits.truncate(limit);
        hits
    };
    let claims_out: Vec<Value> = claims
        .iter()
        .zip(&per_claim)
        .map(|(claim, rows)| {
            json!({
                "claim": claim, "documents_judged": rows.len(),
                "supporting": lists(rows, "supports"),
                "contradicting": lists(rows, "contradicts"),
                "qualifying": lists(rows, "qualifies"),
            })
        })
        .collect();
    let checks = root.join("checks");
    std::fs::create_dir_all(&checks)?;
    let number = std::fs::read_dir(&checks)?.count() + 1;
    let check_path = checks.join(format!("{number:03}.json"));
    std::fs::write(
        &check_path,
        serde_json::to_vec(&json!({"question": question, "claims": claims, "judgments": full}))?,
    )?;
    record_call(
        &root,
        "check",
        json!({"claims": claims, "scope": format!("{scope:?}").to_lowercase()}),
        &usage,
    )?;
    Ok(json!({
        "schema_version": 1, "compact": true, "question": question,
        "session": session_view(&root, &usage),
        "claims": claims_out,
        "documents_failed": failed,
        "check_path": check_path,
        "limitations": ["Probabilities are uncalibrated. Read a row's text_path before you cite it.",
            "Low support does not mean contradiction: the text may not address the claim."],
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_compact_pool_view_lists_only_pools_with_a_signal() {
        let pools = json!([
            {"id":"near","state":"unfetched","route":0.35},
            {"id":"far","state":"unfetched","route":0.21},
            {"id":"live","state":"unscored_tail","route":0.5,"best":0.3},
            {"id":"dead","state":"unscored_tail","route":0.5,"best":0.05},
        ]);
        let summary = pool_summary(
            &pools,
            &json!({"uncertain_threshold":0.15,"fetch_threshold":0.4}),
        );
        let ids: Vec<&str> = summary["actionable"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, ["near", "live"]);
        assert_eq!(summary["unfetched"], 2);
        assert_eq!(summary["unscored_tails"], 2);
        assert_eq!(summary["best_tail_score"], 0.3);
    }

    #[test]
    fn one_call_at_a_time_holds_a_session() {
        let dir = tempfile::tempdir().unwrap();
        let held = lock(dir.path()).unwrap().expect("a free session");
        assert!(lock(dir.path()).unwrap().is_none());
        drop(held);
        assert!(lock(dir.path()).unwrap().is_some());
    }

    #[test]
    fn a_call_that_stopped_counts_its_whole_allowance_as_spent() {
        let dir = tempfile::tempdir().unwrap();
        record_call(dir.path(), "search", json!({}), &Usage::default()).unwrap();
        std::fs::write(
            dir.path().join("usage.json"),
            serde_json::to_vec(&Usage::default()).unwrap(),
        )
        .unwrap();
        begin_call(dir.path(), 0.5).unwrap();
        // The process stops here. The next call settles the mark conservatively.
        begin_call(dir.path(), 0.25).unwrap();
        let usage: Usage = read(dir.path(), "usage.json").unwrap();
        assert_eq!(usage.cost_usd, 0.5);
        end_call(dir.path()).unwrap();
        begin_call(dir.path(), 0.25).unwrap();
        let usage: Usage = read(dir.path(), "usage.json").unwrap();
        assert_eq!(usage.cost_usd, 0.5);
    }

    #[test]
    fn the_budget_cap_is_fixed_at_the_first_call() {
        let dir = tempfile::tempdir().unwrap();
        record_call(dir.path(), "search", json!({}), &Usage::default()).unwrap();
        set_budget_cap(dir.path(), 1.0).unwrap();
        set_budget_cap(dir.path(), 5.0).unwrap();
        assert_eq!(budget_cap(dir.path()).unwrap(), 3.0);
    }
}
