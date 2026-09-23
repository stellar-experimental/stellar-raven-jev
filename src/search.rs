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
    let rerank: Value = read(&root, "rerank.json")?;
    // A run that failed before intent classification ranks as timeless, with no confidence.
    let intent: crate::rank::Intent = if rerank["intent"].is_null() {
        crate::rank::Intent {
            kind: "timeless".into(),
            confidence: 0.0,
            versioned: 0.0,
        }
    } else {
        serde_json::from_value(rerank["intent"].clone())
            .context("rerank.json has an invalid question intent")?
    };
    let target = rerank["target"].as_u64().map(|t| t as u32);
    let mut ranking: BTreeMap<String, crate::rank::Ranked> = BTreeMap::new();
    let document_dir = root.join(format!("search-documents{suffix}"));
    std::fs::create_dir_all(&document_dir)?;
    let mut results = Vec::new();
    for (status, mut documents) in classified(&root)? {
        if status == "selected" {
            let refs: Vec<&Document> = documents.iter().collect();
            let scopes: Vec<String> = refs.iter().map(|d| content_scope(d).to_owned()).collect();
            let ranked = crate::rank::rank(&intent, target, &refs, &scores, &scopes);
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
            let number = results.len() + 1;
            let text_path = document_dir.join(format!("{number:04}.txt"));
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
                    "date": crate::rank::document_date(&document),
                });
            }
            if full_text {
                row["text"] = json!(document.text);
            }
            results.push(row);
        }
    }
    let report = json!({
        "schema_version":1,"question":question["question"],"mode":if question["config"]["fixture"] == true {"fixture"} else {"live"},
        "status":outcome.status,"directory":root,
        "report_path":root.join(format!("search{suffix}.json")),"source_scope":scope,
        "replay_variant":variant,
        "currentness":currentness(&intent, target, &rerank, &results),
        "counts":{"selected":outcome.selected,"uncertain":outcome.uncertain,"rejected":outcome.rejected,"omitted":omitted.len(),"reports":failures.len()},
        "usage":outcome.usage,"results":results,"reports":failures,
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

/// Selected rows that `currentness` reads for the newest dated evidence.
const NEWEST_DATED_WINDOW: usize = 15;

/// What the ranking knows about time: the intent, the leading target and its support, the newest
/// dated evidence among the top-ranked rows, and conflicts between official pages and other
/// sources about the target.
fn currentness(
    intent: &crate::rank::Intent,
    target: Option<u32>,
    rerank: &Value,
    rows: &[Value],
) -> Value {
    let selected: Vec<&Value> = rows.iter().filter(|r| r["status"] == "selected").collect();
    let mut dated: Vec<&Value> = selected
        .iter()
        .take(NEWEST_DATED_WINDOW)
        .copied()
        .filter(|r| {
            r["rank"]["bucket"].as_u64().unwrap_or(2) <= 2
                && matches!(
                    r["rank"]["date"]["kind"].as_str(),
                    Some("published" | "modified")
                )
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
    let mut conflicts = Vec::new();
    if let Some(t) = target {
        let signal = |r: &Value, k: &str| r["rank"]["current"][k].as_f64().unwrap_or(0.0);
        let mentions = |r: &Value| {
            r["rank"]["protocols"]
                .as_array()
                .is_some_and(|p| p.iter().any(|v| v.as_u64() == Some(t as u64)))
        };
        let titles = |rows: Vec<&&Value>| -> Vec<String> {
            let mut titles: Vec<String> = Vec::new();
            for title in rows.iter().filter_map(|r| r["title"].as_str()) {
                if !titles.iter().any(|t| t.eq_ignore_ascii_case(title)) && titles.len() < 5 {
                    titles.push(title.to_owned());
                }
            }
            titles
        };
        let live: Vec<&&Value> = selected
            .iter()
            .filter(|r| r["rank"]["about_target"] == true && signal(r, "live") >= 0.5)
            .collect();
        let newest_live = live
            .iter()
            .filter_map(|r| r["rank"]["date"]["date"].as_str())
            .max();
        // An official page written before the newest live report is expected to call the
        // target planned. Only an undated or newer one conflicts.
        let official_planned = titles(
            selected
                .iter()
                .filter(|r| r["rank"]["authority_tier"] == 1 && mentions(r))
                .filter(|r| signal(r, "planned_only") >= 0.5 && signal(r, "live") < 0.5)
                .filter(
                    |r| match (r["rank"]["date"]["date"].as_str(), newest_live) {
                        (Some(date), Some(newest)) => date >= newest,
                        _ => true,
                    },
                )
                .collect(),
        );
        let reported_live = titles(live);
        if !official_planned.is_empty() && !reported_live.is_empty() {
            conflicts.push(json!({
                "protocol": format!("Protocol {t}"),
                "official_describe_as_planned": official_planned,
                "others_report_live": reported_live,
            }));
        }
    }
    json!({
        "intent": intent,
        "leading_protocol": target.map(|t| format!("Protocol {t}")),
        "leading_protocol_support_clusters": rerank["target_support_clusters"],
        "assessed_documents": selected
            .iter()
            .filter(|r| r["rank"]["current"].as_object().is_some_and(|c| !c.is_empty()))
            .count(),
        "newest_dated_evidence": newest,
        "conflicts": conflicts,
    })
}

/// Higher is more complete. Rosters and full bodies beat excerpts and catalog metadata.
fn scope_rank(document: &Document) -> u8 {
    match content_scope(document) {
        "structured_roster" => 5,
        "published_markdown_main_content"
        | "article_visible_text"
        | "main_visible_text"
        | "skill_markdown_entrypoint"
        | "stored_editorial_body"
        | "stored_application_body"
        | "stored_source_body" => 4,
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
    // When no official page makes the display, the best-ranked official page that is not
    // superseded takes the last slot.
    if limit != 0 {
        let mut shown_urls = BTreeSet::new();
        let shown: Vec<usize> = (0..order.len())
            .filter(|&i| {
                let url = order[i]["url"].as_str().unwrap_or_default();
                url.is_empty() || shown_urls.insert(url.to_owned())
            })
            .take(limit)
            .collect();
        let official = |r: &Value| {
            r["rank"]["authority_tier"] == 1 && r["rank"]["bucket"].as_u64().unwrap_or(2) <= 2
        };
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
            "date":matches!(row["rank"]["date"]["kind"].as_str(), Some("published" | "modified")).then(|| &row["rank"]["date"]["date"]),
            "authority_tier":row["rank"]["authority_tier"],
            "same_url_others":url_counts.get(url).map(|n| n.saturating_sub(1)).unwrap_or(0),
        });
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
            current: BTreeMap::new(),
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
            "rerank.json",
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
            "rerank.json",
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
        assert!(
            scope_rank(&doc("structured_roster"))
                > scope_rank(&doc("published_markdown_main_content"))
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
            current: BTreeMap::new(),
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
}
