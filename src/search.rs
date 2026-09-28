//! Search presentation. Full evidence remains available independently of display limits.
use crate::{
    pipeline::RunOutcome,
    types::{Document, DocumentScore, Failure},
};
use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

fn read<T: serde::de::DeserializeOwned>(root: &Path, name: &str) -> Result<T> {
    Ok(serde_json::from_slice(&std::fs::read(root.join(name))?)?)
}

/// Uncertain documents: length-normalized relevance in whole-percent bands, then content
/// completeness, then ID. Selected documents are ordered by `rank::rank`.
fn uncertain_order(
    scores: &BTreeMap<&str, &DocumentScore>,
    a: &Document,
    b: &Document,
) -> std::cmp::Ordering {
    let key = |d: &Document| {
        scores
            .get(d.id.as_str())
            .map(|s| (s.usable_top2_mean * 100.0).round())
            .unwrap_or(-1.0)
    };
    key(b)
        .total_cmp(&key(a))
        .then(scope_rank(b).cmp(&scope_rank(a)))
        .then(a.id.cmp(&b.id))
}

/// A variant name becomes part of two output paths. Only plain characters are allowed.
fn validate_variant(name: &str) -> Result<()> {
    anyhow::ensure!(
        !name.is_empty()
            && name.len() <= 64
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
            && !name.starts_with('.'),
        "Replay variant names use 1 to 64 letters, digits, '-', '_', or '.', and do not start with '.'"
    );
    Ok(())
}

/// Selected and uncertain documents in their saved order: each admitted document is stored once
/// in documents.json, and classification.json lists IDs by status.
fn classified(root: &Path) -> Result<Vec<(&'static str, Vec<Document>)>> {
    let statuses = ["selected", "uncertain"];
    let classification: Value = read(root, "classification.json")?;
    let mut documents: BTreeMap<String, Document> = BTreeMap::new();
    for document in read::<Vec<Document>>(root, "documents.json")? {
        let id = document.id.clone();
        anyhow::ensure!(
            documents.insert(id.clone(), document).is_none(),
            "documents.json repeats document {id}"
        );
    }
    statuses
        .into_iter()
        .map(|status| {
            let ids = classification[status]
                .as_array()
                .with_context(|| format!("classification.json lacks {status}"))?;
            let list = ids
                .iter()
                .map(|id| {
                    let id = id.as_str().context("Classification IDs must be strings")?;
                    documents
                        .remove(id)
                        .with_context(|| format!("documents.json lacks classified document {id}"))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((status, list))
        })
        .collect()
}

/// Rebuild the report from saved documents and scores. With a variant name, output goes to
/// `search-<variant>.json` and `search-documents-<variant>/`, so the original presentation stays.
/// This lets a ranking change be replayed on a saved run without new retrieval or scoring.
pub fn build_report_variant(
    outcome: &RunOutcome,
    full_text: bool,
    variant: Option<&str>,
) -> Result<Value> {
    let root = outcome.directory.canonicalize()?;
    if let Some(name) = variant {
        validate_variant(name)?;
        for existing in [
            root.join(format!("search-{name}.json")),
            root.join(format!("search-documents-{name}")),
        ] {
            anyhow::ensure!(
                !existing.exists(),
                "Replay variant {name} already exists at {}; choose a new name so old replay evidence stays",
                existing.display()
            );
        }
    }
    let suffix = variant.map(|v| format!("-{v}")).unwrap_or_default();
    let question: Value = read(&root, "question.json")?;
    let scope: Value = read(&root, "source-scope.json")?;
    let scores: Vec<DocumentScore> = read(&root, "scores.json")?;
    let scores: BTreeMap<_, _> = scores.iter().map(|s| (s.document_id.as_str(), s)).collect();
    let failures: Vec<Failure> = read(&root, "failures.json")?;
    let omitted: Vec<Document> = read(&root, "omitted.json")?;
    let intent_record: Value = read(&root, "intent.json")?;
    // A run that failed before intent classification ranks as timeless, with no confidence.
    let intent: crate::rank::Intent = if intent_record["intent"].is_null() {
        crate::rank::Intent {
            kind: "timeless".into(),
            confidence: 0.0,
            versioned: 0.0,
        }
    } else {
        serde_json::from_value(intent_record["intent"].clone())
            .context("intent.json has an invalid question intent")?
    };
    // The run's own reference date, so a later replay ranks the same way.
    let today = question["config"]["today"]
        .as_str()
        .and_then(crate::rank::iso_date_days)
        .or_else(|| crate::rank::iso_date_days(&crate::rank::today_utc()))
        .unwrap_or_default();
    let mut ranking: BTreeMap<String, crate::rank::Ranked> = BTreeMap::new();
    let document_dir = root.join(format!("search-documents{suffix}"));
    std::fs::create_dir_all(&document_dir)?;
    let mut results = Vec::new();
    // A document's text file is named by its position in documents.json, which only grows within
    // a session, so a text_path never changes when later calls re-rank the session.
    let positions: BTreeMap<String, usize> = read::<Vec<Document>>(&root, "documents.json")?
        .into_iter()
        .enumerate()
        .map(|(i, d)| (d.id, i + 1))
        .collect();
    for (status, mut documents) in classified(&root)? {
        if status == "selected" {
            let refs: Vec<&Document> = documents.iter().collect();
            let scopes: Vec<String> = refs.iter().map(|d| content_scope(d).to_owned()).collect();
            let ranked = crate::rank::rank(&intent, &refs, &scores, &scopes, today);
            let position: BTreeMap<&str, usize> = ranked
                .iter()
                .enumerate()
                .map(|(i, r)| (r.id.as_str(), i))
                .collect();
            documents.sort_by_key(|d| position[d.id.as_str()]);
            ranking = ranked.into_iter().map(|r| (r.id.clone(), r)).collect();
        } else {
            documents.sort_by(|a, b| uncertain_order(&scores, a, b));
        }
        for document in documents {
            let text_path = document_dir.join(format!("{:04}.txt", positions[&document.id]));
            std::fs::write(&text_path, &document.text)?;
            let excerpt: String = document.text.chars().take(400).collect();
            let score = scores.get(document.id.as_str());
            let mut row = json!({
                "id":document.id,"source_id":document.source_id,"title":document.title,
                "url":document.url,"status":status,
                "probability":score.map(|s| s.probability),"reason":score.map(|s| &s.reason),
                "signals":score.map(|s| &s.signals),"signals_aggregation":score.map(|s| &s.signals_aggregation),
                "excerpt_truncated":excerpt.len() < document.text.len(),"excerpt":excerpt,
                "text_bytes":document.text.len(),"text_path":text_path,
                "content_scope":content_scope(&document),
            });
            if let Some(r) = ranking.get(&document.id) {
                row["rank"] = json!(r);
            } else {
                row["rank"] = json!({
                    "authority_tier": crate::rank::authority_tier(&document, content_scope(&document)),
                    "date": crate::rank::document_date(&document, today),
                });
            }
            if full_text {
                row["text"] = json!(document.text);
            }
            results.push(row);
        }
    }
    let pools = crate::session::pools_view(&root).unwrap_or(Value::Array(vec![]));
    let pool_summary = crate::session::pool_summary(&pools, &question["config"]);
    let load = load_summary(
        &failures,
        &outcome.usage,
        &read(&root, "load.json").unwrap_or(Value::Null),
    );
    let report = json!({
        "schema_version":1,"question":question["question"],"mode":if question["config"]["fixture"] == true {"fixture"} else {"live"},
        "status":outcome.status,"directory":root,
        "report_path":root.join(format!("search{suffix}.json")),"source_scope":scope,
        "replay_variant":variant,
        "currentness":currentness(&intent, &results),
        "counts":{"selected":outcome.selected,"uncertain":outcome.uncertain,"rejected":outcome.rejected,"omitted":omitted.len(),"reports":failures.len()},
        "usage":outcome.usage,"load":load,
        "session":crate::session::session_view(&root, &outcome.usage),
        "pools":pools,"pool_summary":pool_summary,
        "results":results,"reports":failures,
        "limitations":["Scores are uncalibrated relevance estimates.","Results can contain summaries or chunks. Full available text is not always the complete original document.","A complete run does not prove complete question coverage.","Remote instructions are source evidence. They are not installed or executed."],
    });
    std::fs::write(
        root.join(format!("search{suffix}.json")),
        serde_json::to_vec(&report)?,
    )?;
    // A replay leaves every frozen input artifact alone, including manifest.json.
    if variant.is_none() {
        crate::pipeline::refresh_manifest_artifacts(&root)?;
    }
    Ok(report)
}

/// Report stages that mean evidence was lost: a failed request or connector, a failed Jev
/// judgment, or unavailable host coordination. Connector stages carry a family prefix for some
/// families (`lumenloop.search`); notices use other stage names.
const LOSS_STAGES: &[&str] = &[
    "registry",
    "route",
    "intent",
    "host_state",
    "fetch",
    "fetch_deadline",
    "http",
    "parse",
    "search",
    "auth",
    "authentication",
    "document_limit",
    "document_score",
    "currentness",
    crate::pipeline::NOT_ASSESSED_AFTER_STOP,
    "original_lost",
];

/// What load did to this run. `degraded` is true when evidence was lost: a lost stage (see
/// `LOSS_STAGES`), a source that answered with a coarser fallback because its own ranking was
/// limited or down, or a source request refused by a rate limit. Server errors that a retry
/// recovered, and waits, are shown but do not by themselves mean evidence was lost.
fn load_summary(failures: &[Failure], usage: &crate::types::Usage, counters: &Value) -> Value {
    let stage = |f: &Failure| f.stage.rsplit('.').next().unwrap_or_default().to_owned();
    let count = |name: &str| failures.iter().filter(|f| stage(f) == name).count();
    let counter = |name: &str| counters[name].as_u64().unwrap_or(0);
    let lost = failures
        .iter()
        .filter(|f| LOSS_STAGES.contains(&stage(f).as_str()))
        .count();
    let fallback = count("search_limit");
    let refused = counter("source_rate_limited_requests");
    let mut causes: BTreeMap<&str, usize> = BTreeMap::new();
    for cause in failures.iter().filter_map(|f| f.cause.as_deref()) {
        *causes.entry(cause).or_default() += 1;
    }
    json!({
        "degraded": lost + fallback > 0 || refused > 0,
        "lost_evidence_reports": lost,
                "sources_cut_at_deadline": count("fetch_deadline"),
        "cut_sources": failures
            .iter()
            .filter(|f| stage(f) == "fetch_deadline")
            .filter_map(|f| f.source_id.clone())
            .collect::<Vec<_>>(),
        "source_fallback_responses": fallback,
        "source_rate_limited_requests": refused,
        "source_server_errors": counter("source_server_errors"),
        "source_gate_wait_ms": counter("source_gate_wait_ms"),
                "source_booking_wait_ms": counter("source_booking_wait_ms"),
        "source_slot_wait_ms": counter("source_slot_wait_ms"),
        "source_requests": counters["source_requests"],
        "source_latency": counters["source_latency"],
        "original_reads": counters["original_reads"],
        "scoring_failures": count("document_score"),
        "currentness_failures": count("currentness"),
        "not_assessed_after_stop": count(crate::pipeline::NOT_ASSESSED_AFTER_STOP),
        "jev_failure_causes": causes,
        "jev_rate_limited_requests": usage.rate_limited_requests,
        "jev_wait_ms": usage.provider_wait_ms,
    })
}

#[cfg(test)]
mod load_tests {
    use super::*;

    fn report(stage: &str) -> Failure {
        Failure {
            stage: stage.into(),
            source_id: Some("s".into()),
            message: "m".into(),
            cause: None,
        }
    }

    #[test]
    fn only_lost_evidence_marks_a_run_degraded() {
        let usage = crate::types::Usage::default();
        for notice in [
            "coverage",
            "truncation",
            "query_plan",
            "search_notice",
            "lumenloop.search_message",
            "lumenloop.search_annotation",
            "upstream",
            "original",
        ] {
            let load = load_summary(&[report(notice)], &usage, &Value::Null);
            assert_eq!(load["degraded"], false, "{notice}");
        }
        for loss in [
            "fetch_deadline",
            "fetch",
            "route",
            "http",
            "parse",
            "search",
            "lumenloop.search",
            "lumenloop.search_limit",
            "search_limit",
            "document_score",
            "document_limit",
            "original_lost",
            "currentness",
            "not_assessed_after_stop",
            "host_state",
        ] {
            let load = load_summary(&[report(loss)], &usage, &Value::Null);
            assert_eq!(load["degraded"], true, "{loss}");
        }
        let refused = load_summary(&[], &usage, &json!({"source_rate_limited_requests": 1}));
        assert_eq!(refused["degraded"], true);
        let recovered = load_summary(&[], &usage, &json!({"source_server_errors": 3}));
        assert_eq!(recovered["degraded"], false);
    }
}

/// Selected rows that `currentness` reads for the newest dated evidence.
const NEWEST_DATED_WINDOW: usize = 15;

/// What the ranking knows about time: the intent and the newest dated evidence among the
/// top-ranked rows.
fn currentness(intent: &crate::rank::Intent, rows: &[Value]) -> Value {
    let selected: Vec<&Value> = rows.iter().filter(|r| r["status"] == "selected").collect();
    let mut dated: Vec<&Value> = selected
        .iter()
        .take(NEWEST_DATED_WINDOW)
        .copied()
        .filter(|r| {
            r["rank"]["date"]["kind"]
                .as_str()
                .is_some_and(crate::rank::drives_recency)
        })
        .collect();
    dated.sort_by(|a, b| {
        b["rank"]["date"]["date"]
            .as_str()
            .cmp(&a["rank"]["date"]["date"].as_str())
    });
    let mut seen = BTreeSet::new();
    let newest: Vec<Value> = if intent.asks_currentness() { dated } else { Vec::new() }
        .iter()
        .filter(|r| seen.insert(r["title"].as_str().unwrap_or_default().to_lowercase()))
        .take(3)
        .map(|r| json!({"title":r["title"],"url":r["url"],"date":r["rank"]["date"]["date"],"authority_tier":r["rank"]["authority_tier"]}))
        .collect();
    json!({
        "intent": intent,
        "assessed_documents": selected
            .iter()
            .filter(|r| r["rank"]["still_current"].is_number())
            .count(),
        "newest_dated_evidence": newest,
    })
}

/// Higher is more complete. Rosters and full bodies beat excerpts and catalog metadata.
fn scope_rank(document: &Document) -> u8 {
    match content_scope(document) {
        "structured_roster"
        | "published_markdown_main_content"
        | "article_visible_text"
        | "main_visible_text"
        | "skill_markdown_entrypoint"
        | "stored_editorial_body"
        | "stored_application_body"
        | "stored_source_body"
        | "published_plain_text" => 4,
        "structured_record_with_detail" | "research_chunk" | "transcript_excerpt" => 3,
        "structured_record" | "ai_summary" => 2,
        "indexed_sections_or_metadata"
        | "catalog_metadata"
        | "project_metadata"
        | "job_catalog_metadata" => 1,
        _ => 0,
    }
}

/// Connectors name the content scope differently. LumenLoop records use `content_kind`.
/// A Scout row that says `synthetic: true` is generated context, not original page text, even when
/// it carries an official URL. It is reported as `synthetic_record` from the preserved provenance,
/// so saved runs show it on replay without any change to the stored text or provenance.
pub(crate) fn content_scope(document: &Document) -> &str {
    if document.provenance["row"]["synthetic"] == true {
        return "synthetic_record";
    }
    document.provenance["content_scope"]
        .as_str()
        .or_else(|| document.provenance["content_kind"].as_str())
        .unwrap_or("")
}

/// Project the full report into a small agent response. The saved `search.json` stays complete.
/// Keeps the highest-ranked selected result per URL. Uncertain rows stay in the full report only.
/// Write `bundle.md` in the session folder: the full text of every shown result in rank order,
/// each followed by its companions, under a header that names the rank, URL, scope, date, and
/// text path. One file, so a reader need not open each `text_path`. A text file shared by two
/// rows is written once.
pub fn write_bundle(compact: &Value, root: &std::path::Path) -> anyhow::Result<std::path::PathBuf> {
    use std::fmt::Write as _;
    // Sections first, each with its line offset in the body, then a contents list that gives
    // every section's line, so a reader can print one section with `sed -n 'START,ENDp'`.
    let mut body = String::new();
    let mut contents: Vec<(String, usize)> = Vec::new();
    let mut written = BTreeSet::new();
    let mut add = |body: &mut String,
                   contents: &mut Vec<(String, usize)>,
                   label: String,
                   row: &Value,
                   url: &Value|
     -> anyhow::Result<()> {
        let Some(path) = row["text_path"].as_str() else {
            return Ok(());
        };
        if !written.insert(path.to_owned()) {
            return Ok(());
        }
        let text = std::fs::read_to_string(path).unwrap_or_default();
        let heading = format!("{label}: {}", row["title"].as_str().unwrap_or("(same URL)"));
        contents.push((heading.clone(), body.lines().count()));
        writeln!(
            body,
            "## {heading}\nurl: {} | scope: {} | date: {} | text_path: {path}\n\n{}\n",
            url.as_str().unwrap_or("none"),
            row["content_scope"].as_str().unwrap_or("unknown"),
            row["date"].as_str().unwrap_or("none"),
            text.trim_end()
        )?;
        Ok(())
    };
    for (rank, row) in compact["results"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
    {
        add(
            &mut body,
            &mut contents,
            format!("Rank {}", rank + 1),
            row,
            &row["url"],
        )?;
        for companion in row["companions"].as_array().into_iter().flatten() {
            let label = format!("Rank {} companion", rank + 1);
            add(&mut body, &mut contents, label, companion, &row["url"])?;
        }
    }
    // Title, blank, "Contents", one line per section, blank: the body starts after these.
    let head_lines = 4 + contents.len();
    let mut out = format!(
        "# Evidence for: {}\n\nContents (section: first line):\n",
        compact["question"].as_str().unwrap_or_default()
    );
    for (heading, offset) in &contents {
        writeln!(out, "- {heading}: line {}", head_lines + offset + 1)?;
    }
    out.push('\n');
    out.push_str(&body);
    let path = root.join("bundle.md");
    std::fs::write(&path, out)?;
    Ok(path)
}

/// At most this many other same-URL rows are listed with each compact row.
const COMPANIONS: usize = 2;

pub fn compact_report(report: &Value, limit: usize) -> Value {
    let mut seen = BTreeSet::new();
    let mut duplicate_urls = 0usize;
    let mut results = Vec::new();
    let rows: Vec<&Value> = report["results"].as_array().into_iter().flatten().collect();
    // Other selected results at the same URL stay discoverable through this count and the full report.
    let mut url_counts: BTreeMap<&str, usize> = BTreeMap::new();
    for row in &rows {
        if row["status"] == "selected" {
            if let Some(url) = row["url"].as_str().filter(|u| !u.is_empty()) {
                *url_counts.entry(url).or_default() += 1;
            }
        }
    }
    // `results` is already ordered: selected by rank, then uncertain. A selected row without a URL
    // that has the same title as a row with a URL is a copy of it: the row with the URL takes the
    // better position, and the copy counts as a duplicate.
    let selected: Vec<&Value> = rows
        .iter()
        .copied()
        .filter(|r| r["status"] == "selected")
        .collect();
    let title_key = |r: &Value| -> Option<String> {
        let key: String = r["title"]
            .as_str()?
            .to_lowercase()
            .chars()
            .filter(|c| c.is_alphanumeric())
            .collect();
        (key.chars().count() >= 12).then_some(key)
    };
    let url_of = |r: &Value| r["url"].as_str().unwrap_or_default().to_owned();
    let mut order: Vec<&Value> = Vec::new();
    let mut moved = BTreeSet::new();
    for (i, row) in selected.iter().enumerate() {
        if moved.contains(&i) {
            continue;
        }
        let copy_of = url_of(row)
            .is_empty()
            .then(|| title_key(row))
            .flatten()
            .and_then(|key| {
                selected
                    .iter()
                    .position(|o| !url_of(o).is_empty() && title_key(o).as_ref() == Some(&key))
            });
        match copy_of {
            Some(j) if j > i && moved.insert(j) => order.push(selected[j]),
            Some(_) => duplicate_urls += 1,
            None => order.push(row),
        }
    }
    duplicate_urls += moved.len();
    let unique_selected =
        url_counts.len() + order.iter().filter(|row| url_of(row).is_empty()).count();
    // The other selected rows at each URL, longest first: a short chunk can stand for a URL whose
    // full page is also selected, and the reader needs a path to it.
    let mut same_url: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
    for row in &order {
        let url = url_of(row);
        if !url.is_empty() {
            same_url.entry(url).or_default().push(row);
        }
    }
    for rows in same_url.values_mut() {
        rows.sort_by_key(|r| std::cmp::Reverse(r["text_bytes"].as_u64().unwrap_or(0)));
    }
    let companions = |row: &Value| -> Vec<Value> {
        same_url
            .get(&url_of(row))
            .into_iter()
            .flatten()
            .filter(|other| !std::ptr::eq(**other, row))
            // A same-size, same-scope copy adds nothing the shown row lacks.
            .filter(|other| {
                other["text_bytes"] != row["text_bytes"]
                    || other["content_scope"] != row["content_scope"]
            })
            .take(COMPANIONS)
            .map(|other| {
                json!({"content_scope":other["content_scope"],"text_bytes":other["text_bytes"],
                    "text_path":other["text_path"],"source_id":other["source_id"]})
            })
            .collect()
    };
    // Among rows that share a URL, the one with the best authority tier stands for the URL, at the
    // position of the first one. An official page thus never hides behind a summary of itself.
    let tier = |r: &Value| r["rank"]["authority_tier"].as_u64().unwrap_or(3);
    let mut best: BTreeMap<String, usize> = BTreeMap::new();
    for (i, row) in order.iter().enumerate() {
        let url = url_of(row);
        if url.is_empty() {
            continue;
        }
        let entry = best.entry(url).or_insert(i);
        if tier(row) < tier(order[*entry]) {
            *entry = i;
        }
    }
    let mut first: BTreeMap<String, usize> = BTreeMap::new();
    for (i, row) in order.iter().enumerate() {
        let url = url_of(row);
        if !url.is_empty() {
            first.entry(url).or_insert(i);
        }
    }
    for (url, i) in &first {
        order.swap(*i, best[url]);
    }
    // When no official page makes the display, the best-ranked official page takes the last slot.
    if limit != 0 {
        let mut shown_urls = BTreeSet::new();
        let shown: Vec<usize> = (0..order.len())
            .filter(|&i| {
                let url = order[i]["url"].as_str().unwrap_or_default();
                url.is_empty() || shown_urls.insert(url.to_owned())
            })
            .take(limit)
            .collect();
        let official = |r: &Value| r["rank"]["authority_tier"] == 1;
        if shown.len() == limit && !shown.iter().any(|&i| official(order[i])) {
            if let Some(pick) = (shown[limit - 1] + 1..order.len())
                .find(|&i| official(order[i]) && !shown_urls.contains(&url_of(order[i])))
            {
                let row = order.remove(pick);
                order.insert(shown[limit - 1], row);
            }
        }
    }
    for row in order {
        let url = row["url"].as_str().unwrap_or_default();
        if !url.is_empty() && !seen.insert(url.to_owned()) {
            duplicate_urls += 1;
            continue;
        }
        if limit != 0 && results.len() >= limit {
            continue;
        }
        let mut compact = json!({
            "title":row["title"],"url":row["url"],"source_id":row["source_id"],
            "status":row["status"],
            "probability":row["probability"],"excerpt":row["excerpt"],
            "text_bytes":row["text_bytes"],"text_path":row["text_path"],
            "content_scope":row["content_scope"],
            "date":row["rank"]["date"]["kind"].as_str().is_some_and(crate::rank::drives_recency).then(|| &row["rank"]["date"]["date"]),
            "date_kind":row["rank"]["date"]["kind"].as_str().filter(|k| crate::rank::drives_recency(k)),
            "still_current":row["rank"]["still_current"],
            "authority_tier":row["rank"]["authority_tier"],
                        "same_url_others":url_counts.get(url).map(|n| n.saturating_sub(1)).unwrap_or(0),
        });
        let others = companions(row);
        if !others.is_empty() {
            compact["companions"] = json!(others);
        }
        if let Some(text) = row.get("text") {
            compact["text"] = text.clone();
        }
        results.push(compact);
    }
    let mut stages: BTreeMap<String, usize> = BTreeMap::new();
    for item in report["reports"].as_array().into_iter().flatten() {
        *stages
            .entry(item["stage"].as_str().unwrap_or("unknown").to_owned())
            .or_default() += 1;
    }
    json!({
        "schema_version":1,"compact":true,"question":report["question"],"mode":report["mode"],
        "status":report["status"],"counts":report["counts"],"usage":report["usage"],
        "load":report["load"],"session":report["session"],"pools":report["pool_summary"],
        "source_scope":report["source_scope"]["scope"],
        "currentness":report["currentness"],
        "results":results,
        "not_shown":{
            "selected_beyond_limit":unique_selected.saturating_sub(results.len()),
            "duplicate_urls":duplicate_urls,"uncertain":report["counts"]["uncertain"],
        },
        "report_stage_counts":stages,
        "full_report_path":report["report_path"],
        "limitations":report["limitations"],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_replay_variant_writes_its_own_report_and_leaves_the_original() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let write = |name: &str, value: &Value| {
            std::fs::write(root.join(name), serde_json::to_vec(value).unwrap()).unwrap()
        };
        let doc = |id: &str| Document {
            id: id.into(),
            source_id: "s".into(),
            title: id.into(),
            url: format!("https://x/{id}"),
            text: format!("text {id}"),
            provenance: json!({"content_scope":"research_chunk"}),
            raw_artifacts: vec![],
        };
        let score = |id: &str, p: f64| DocumentScore {
            document_id: id.into(),
            probability: p,
            reason: "fixture".into(),
            signals: BTreeMap::from([("usable_evidence".into(), p)]),
            signals_aggregation: "independent_max_per_signal_across_chunks".into(),
            best_chunk: [0, 0],
            usable_top2_mean: p,
            still_current: None,
        };
        write(
            "question.json",
            &json!({"question":"q","config":{"fixture":true}}),
        );
        write("source-scope.json", &json!({"scope":"all"}));
        write("scores.json", &json!([score("a", 0.5), score("b", 0.9)]));
        write("failures.json", &json!([]));
        write("omitted.json", &json!([]));
        write(
            "intent.json",
            &json!({"intent":{"kind":"timeless","confidence":1.0,"versioned":0.0}}),
        );
        write("documents.json", &json!([doc("a"), doc("b")]));
        write(
            "classification.json",
            &json!({"selected":["a","b"],"uncertain":[],"rejected":[]}),
        );
        std::fs::write(root.join("search.json"), b"original").unwrap();
        let outcome = RunOutcome {
            directory: root.to_path_buf(),
            status: "complete".into(),
            selected: 2,
            rejected: 0,
            uncertain: 0,
            failures: 0,
            usage: Default::default(),
            retry_after_ms: None,
        };
        let report = build_report_variant(&outcome, false, Some("t")).unwrap();
        assert_eq!(report["results"][0]["id"], "b");
        assert_eq!(report["results"][0]["signals"]["usable_evidence"], 0.9);
        assert!(root.join("search-t.json").exists());
        assert!(root.join("search-documents-t").is_dir());
        assert_eq!(
            std::fs::read(root.join("search.json")).unwrap(),
            b"original"
        );
        assert!(
            build_report_variant(&outcome, false, Some("t")).is_err(),
            "an existing variant is never overwritten"
        );
    }

    #[test]
    fn report_reads_the_single_store_by_classification_order() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let write = |name: &str, value: &Value| {
            std::fs::write(root.join(name), serde_json::to_vec(value).unwrap()).unwrap()
        };
        let doc = |id: &str| {
            json!({"id":id,"source_id":"s","title":id,"url":format!("https://x/{id}"),
                   "text":format!("text {id}"),"provenance":{},"raw_artifacts":[]})
        };
        write(
            "question.json",
            &json!({"question":"q","config":{"fixture":true}}),
        );
        write("source-scope.json", &json!({"scope":"all"}));
        write("scores.json", &json!([]));
        write("failures.json", &json!([]));
        write("omitted.json", &json!([]));
        write(
            "intent.json",
            &json!({"intent":{"kind":"timeless","confidence":1.0,"versioned":0.0}}),
        );
        write("documents.json", &json!([doc("r"), doc("u"), doc("s")]));
        write(
            "classification.json",
            &json!({"selected":["s"],"uncertain":["u"],"rejected":["r"]}),
        );
        let outcome = RunOutcome {
            directory: root.to_path_buf(),
            status: "complete".into(),
            selected: 1,
            rejected: 1,
            uncertain: 1,
            failures: 0,
            usage: Default::default(),
            retry_after_ms: None,
        };
        let report = build_report_variant(&outcome, false, Some("n")).unwrap();
        let rows = report["results"].as_array().unwrap();
        assert_eq!(rows.len(), 2, "rejected documents stay out of the report");
        assert_eq!(
            (rows[0]["id"].as_str(), rows[0]["status"].as_str()),
            (Some("s"), Some("selected"))
        );
        assert_eq!(
            (rows[1]["id"].as_str(), rows[1]["status"].as_str()),
            (Some("u"), Some("uncertain"))
        );
        write(
            "classification.json",
            &json!({"selected":["missing"],"uncertain":[],"rejected":[]}),
        );
        assert!(build_report_variant(&outcome, false, Some("m")).is_err());
    }

    #[test]
    fn variant_names_are_plain_and_bounded() {
        for ok in ["replay", "item2-v6", "a.b_c"] {
            assert!(validate_variant(ok).is_ok(), "{ok}");
        }
        for bad in ["", "../x", "a/b", ".hidden", "sp ace", &"x".repeat(65)] {
            assert!(validate_variant(bad).is_err(), "{bad:?}");
        }
    }
    #[test]
    fn scope_rank_prefers_complete_content() {
        let doc = |scope: &str| Document {
            id: "x".into(),
            source_id: "s".into(),
            title: String::new(),
            url: String::new(),
            text: String::new(),
            provenance: json!({"content_scope":scope}),
            raw_artifacts: vec![],
        };
        assert_eq!(
            scope_rank(&doc("structured_roster")),
            scope_rank(&doc("published_markdown_main_content"))
        );
        assert!(
            scope_rank(&doc("research_chunk")) > scope_rank(&doc("indexed_sections_or_metadata"))
        );
        assert_eq!(scope_rank(&doc("unknown")), 0);
        let lumenloop = Document {
            id: "x".into(),
            source_id: "s".into(),
            title: String::new(),
            url: String::new(),
            text: String::new(),
            provenance: json!({"content_kind":"stored_editorial_body"}),
            raw_artifacts: vec![],
        };
        assert_eq!(scope_rank(&lumenloop), 4);
        assert_eq!(content_scope(&lumenloop), "stored_editorial_body");
        // A synthetic Scout row keeps its text and provenance but is labeled as generated context.
        let synthetic = Document {
            id: "s".into(),
            source_id: "stellarlight.rfps".into(),
            title: String::new(),
            url: "https://stellar.org/official".into(),
            text: "generated summary".into(),
            provenance: json!({"content_scope":"structured_record","row":{"synthetic":true,"title":"t"}}),
            raw_artifacts: vec![],
        };
        assert_eq!(content_scope(&synthetic), "synthetic_record");
        assert_eq!(scope_rank(&synthetic), 0);
        assert_eq!(
            synthetic.provenance["content_scope"], "structured_record",
            "stored provenance unchanged"
        );
        let not_flagged = Document {
            provenance: json!({"content_scope":"structured_record","row":{"synthetic":false}}),
            ..synthetic.clone()
        };
        assert_eq!(content_scope(&not_flagged), "structured_record");
        let absent = Document {
            provenance: json!({"content_scope":"structured_record","row":{}}),
            ..synthetic.clone()
        };
        assert_eq!(content_scope(&absent), "structured_record");
    }

    #[test]
    fn uncertain_ties_break_by_completeness_then_id() {
        let document = |id: &str, scope: &str| Document {
            id: id.into(),
            source_id: "algolia:docs".into(),
            title: String::new(),
            url: String::new(),
            text: String::new(),
            provenance: json!({"content_scope":scope}),
            raw_artifacts: vec![],
        };
        let article = document("z", "article_visible_text");
        let catalog = document("a", "catalog_metadata");
        let score = |id: &str| DocumentScore {
            document_id: id.into(),
            probability: 0.3,
            reason: String::new(),
            signals: BTreeMap::new(),
            signals_aggregation: String::new(),
            best_chunk: [0, 0],
            usable_top2_mean: 0.3,
            still_current: None,
        };
        let (sa, sc) = (score("z"), score("a"));
        let scores = BTreeMap::from([("z", &sa), ("a", &sc)]);
        assert_eq!(
            uncertain_order(&scores, &article, &catalog),
            std::cmp::Ordering::Less,
            "the complete article sorts before catalog metadata"
        );
    }
    #[test]
    fn compact_projection_keeps_best_url_drops_uncertain_and_counts_reports() {
        let row = |title: &str, url: &str, status: &str, p: f64| {
            json!({"title":title,"url":url,"status":status,"probability":p,"excerpt":"e","text_bytes":1,"text_path":"/t",
            "content_scope": if title == "best" { "synthetic_record" } else { "structured_record" }})
        };
        let report = json!({
            "question":"q","status":"partial","counts":{"uncertain":1},
            "source_scope":{"scope":"all","eligible_source_ids":["a","b"]},
            "results":[row("best","https://x/1","selected",0.9),row("copy","https://x/1","selected",0.8),
                row("second","https://x/2","selected",0.7),row("third","https://x/3","selected",0.6),
                row("maybe","https://x/4","uncertain",0.2)],
            "reports":[{"stage":"coverage"},{"stage":"coverage"},{"stage":"http"}],
        });
        let compact = compact_report(&report, 2);
        let titles: Vec<_> = compact["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["title"].as_str().unwrap())
            .collect();
        assert_eq!(titles, vec!["best", "second"]);
        assert_eq!(
            compact["not_shown"],
            json!({"selected_beyond_limit":1,"duplicate_urls":1,"uncertain":1})
        );
        assert_eq!(
            compact["report_stage_counts"],
            json!({"coverage":2,"http":1})
        );
        assert_eq!(compact["source_scope"], "all");
        assert!(compact.get("reports").is_none());
        assert_eq!(compact["results"][0]["same_url_others"], 1);
        assert_eq!(compact["results"][0]["content_scope"], "synthetic_record");
        assert_eq!(compact["results"][1]["same_url_others"], 0);
        // Uncertain rows never enter the compact list, even with free slots.
        let wide = compact_report(&report, 10);
        assert!(wide["results"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["status"] == "selected"));
        assert_eq!(wide["results"].as_array().unwrap().len(), 3);
        assert_eq!(wide["not_shown"]["selected_beyond_limit"], 0);
        assert_eq!(wide["not_shown"]["uncertain"], 1);
        assert_eq!(
            compact_report(&report, 0)["results"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
    }

    #[test]
    fn compact_counts_unique_selected_urls_and_keeps_url_less_rows() {
        let report = json!({
            "counts":{"uncertain":2},
            "results":[
                {"title":"best","url":"https://x/1","status":"selected"},
                {"title":"copy","url":"https://x/1","status":"selected"},
                {"title":"same-url uncertain","url":"https://x/1","status":"uncertain"},
                {"title":"distinct uncertain","url":"https://x/2","status":"uncertain"}
            ]
        });
        for limit in [2, 1, 0] {
            let compact = compact_report(&report, limit);
            let results = compact["results"].as_array().unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0]["title"], "best");
            assert_eq!(results[0]["same_url_others"], 1);
            assert_eq!(compact["not_shown"]["duplicate_urls"], 1);
            assert_eq!(compact["not_shown"]["selected_beyond_limit"], 0);
            assert_eq!(compact["not_shown"]["uncertain"], 2);
        }
        // URL-less selected rows remain distinct display entries.
        let without_urls = json!({"results":[
            {"title":"first","url":"","status":"selected"},
            {"title":"second","url":"","status":"selected"},
            {"title":"maybe","url":"https://x/2","status":"uncertain"}
        ]});
        let compact = compact_report(&without_urls, 2);
        assert_eq!(compact["results"].as_array().unwrap().len(), 2);
        assert_eq!(compact["results"][1]["title"], "second");
        assert!(compact["not_shown"]["uncertain"].is_null());
        // A URL-less copy of a titled row yields its position to the row with the URL.
        let copies = json!({"results":[
            {"title":"Weekly Roundup: Sep 11","url":"","status":"selected"},
            {"title":"Other page title","url":"https://x/1","status":"selected"},
            {"title":"weekly roundup sep 11","url":"https://x/r","status":"selected"},
            {"title":"Weekly Roundup: Sep 11","url":"","status":"selected"}
        ]});
        let compact = compact_report(&copies, 0);
        let urls: Vec<_> = compact["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["url"].as_str().unwrap())
            .collect();
        assert_eq!(urls, ["https://x/r", "https://x/1"]);
        assert_eq!(compact["not_shown"]["duplicate_urls"], 2);
        assert_eq!(compact["not_shown"]["selected_beyond_limit"], 0);
    }

    #[test]
    fn a_promoted_official_row_replaces_a_shown_copy_of_its_url() {
        let row = |title: &str, url: &str, tier: u8| json!({"title":title,"url":url,"status":"selected","rank":{"authority_tier":tier,"bucket":2}});
        let report = json!({"results":[
            row("synthetic copy","https://developers.stellar.org/a",4),
            row("blog","https://x/b",3),
            row("official","https://developers.stellar.org/a",1)
        ]});
        let compact = compact_report(&report, 2);
        let tiers: Vec<_> = compact["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["authority_tier"].as_u64().unwrap())
            .collect();
        assert_eq!(tiers, [1, 3]);
        assert_eq!(compact["not_shown"]["duplicate_urls"], 1);
        // The official copy inside the window, with a limit the list does not fill.
        let report = json!({"results":[
            row("synthetic copy","https://developers.stellar.org/a",4),
            row("official","https://developers.stellar.org/a",1),
            row("blog","https://x/b",3)
        ]});
        for limit in [2, 10, 0] {
            let compact = compact_report(&report, limit);
            assert_eq!(compact["results"][0]["authority_tier"], 1);
            assert_eq!(compact["results"][0]["title"], "official");
            assert_eq!(compact["not_shown"]["duplicate_urls"], 1);
        }
    }

    #[test]
    fn a_shown_chunk_lists_its_longer_same_url_rows_as_companions() {
        let row = |scope: &str, url: &str, bytes: u64, path: &str| {
            json!({"title":"Arbor ledger receipts","url":url,"status":"selected","source_id":"s",
                "content_scope":scope,"text_bytes":bytes,"text_path":path,"rank":{"authority_tier":1}})
        };
        let page = "https://arbor.example/receipts";
        let report = json!({"results":[
            row("research_chunk", page, 2_000, "/d/0000.txt"),
            row("research_chunk", "https://arbor.example/other", 900, "/d/0001.txt"),
            row("published_markdown_main_content", page, 12_000, "/d/0002.txt"),
            row("indexed_sections_or_metadata", page, 700, "/d/0003.txt"),
            row("main_visible_text", page, 7_000, "/d/0004.txt"),
            row("research_chunk", page, 2_000, "/d/0005.txt"),
        ]});
        let compact = compact_report(&report, 10);
        let first = &compact["results"][0];
        // The top-ranked chunk still stands for its URL; nothing is substituted.
        assert_eq!(first["text_path"], "/d/0000.txt");
        assert_eq!(first["same_url_others"], 4);
        let paths: Vec<_> = first["companions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["text_path"].as_str().unwrap())
            .collect();
        // Longest first, at most two; a same-size, same-scope copy is not listed.
        assert_eq!(paths, ["/d/0002.txt", "/d/0004.txt"]);
        assert_eq!(
            first["companions"][0]["content_scope"],
            "published_markdown_main_content"
        );
        assert!(compact["results"][1].get("companions").is_none());
    }
}
