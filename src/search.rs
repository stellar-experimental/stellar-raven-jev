//! Search presentation. Full evidence remains available independently of display limits.
use crate::{
    pipeline::RunOutcome,
    types::{Document, DocumentScore, Failure},
};
use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::{collections::BTreeMap, fmt::Write as _, path::Path};

fn read<T: serde::de::DeserializeOwned>(root: &Path, name: &str) -> Result<T> {
    Ok(serde_json::from_slice(&std::fs::read(root.join(name))?)?)
}

/// How results are ordered. `Banded` rounds scores to whole percent so near-ties fall to content
/// completeness, then ID; `Raw` orders by the exact score and uses those keys only on exact ties.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RankPolicy {
    #[default]
    Banded,
    Raw,
}

impl RankPolicy {
    fn key(self, value: f64) -> f64 {
        match self {
            Self::Banded => (value * 100.0).round(),
            Self::Raw => value,
        }
    }
}

/// Ordering for one policy over (usable_evidence, scope rank, id). Higher scores first.
/// Missing scores sort last. Pure, so tests can check inversions without a run directory.
pub fn compare_ranked(
    policy: RankPolicy,
    a: (Option<f64>, u8, &str),
    b: (Option<f64>, u8, &str),
) -> std::cmp::Ordering {
    let key = |v: Option<f64>| v.map(|v| policy.key(v)).unwrap_or(-1.0);
    key(b.0)
        .total_cmp(&key(a.0))
        .then(b.1.cmp(&a.1))
        .then(a.2.cmp(b.2))
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

/// Selected and uncertain documents in their saved order. Current runs store each admitted
/// document once in documents.json and list IDs by status in classification.json. Runs saved
/// before that layout keep full documents in selected.json and uncertain.json.
fn classified(root: &Path) -> Result<Vec<(&'static str, Vec<Document>)>> {
    let statuses = ["selected", "uncertain"];
    if !root.join("classification.json").is_file() {
        return statuses
            .into_iter()
            .map(|status| Ok((status, read(root, &format!("{status}.json"))?)))
            .collect();
    }
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
/// This lets a newer ranking be replayed on an older run without new retrieval or scoring.
pub fn build_report_variant(
    outcome: &RunOutcome,
    full_text: bool,
    variant: Option<&str>,
    rank_policy: RankPolicy,
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
    let document_dir = root.join(format!("search-documents{suffix}"));
    std::fs::create_dir_all(&document_dir)?;
    let mut results = Vec::new();
    for (status, mut documents) in classified(&root)? {
        // Order by usable_evidence, then content completeness, then ID. The rank policy decides
        // whether near-ties count as ties.
        documents.sort_by(|a, b| {
            let probability = |d: &Document| scores.get(d.id.as_str()).map(|s| s.probability);
            compare_ranked(
                rank_policy,
                (probability(a), scope_rank(a), &a.id),
                (probability(b), scope_rank(b), &b.id),
            )
        });
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
        "replay_variant":variant,"rank_policy":rank_policy,
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
fn content_scope(document: &Document) -> &str {
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
    let mut seen = std::collections::BTreeSet::new();
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
    let unique_selected = url_counts.len()
        + rows
            .iter()
            .filter(|row| {
                row["status"] == "selected" && row["url"].as_str().unwrap_or_default().is_empty()
            })
            .count();
    // `results` is already ordered by status, then by descending score.
    for row in &rows {
        if row["status"] != "selected" {
            continue;
        }
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

// Keep untrusted source text from sending terminal control sequences.
fn plain(text: &str) -> String {
    text.chars()
        .map(|c| {
            if c.is_control() || matches!(c, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}') {
                ' '
            } else {
                c
            }
        })
        .collect()
}

pub fn render_text(report: &Value, limit: usize) -> String {
    let mut output = String::new();
    let field = |v: &Value| plain(v.as_str().unwrap_or("unknown"));
    let _ = writeln!(
        output,
        "Question: {}\nStatus: {} ({})",
        field(&report["question"]),
        field(&report["status"]),
        field(&report["mode"])
    );
    let _ = writeln!(
        output,
        "Selected: {} | Uncertain: {} | Rejected: {} | Omitted: {} | Reports: {}",
        report["counts"]["selected"],
        report["counts"]["uncertain"],
        report["counts"]["rejected"],
        report["counts"]["omitted"],
        report["counts"]["reports"]
    );
    let _ = writeln!(
        output,
        "Sources: {}\n{}",
        field(&report["source_scope"]["scope"]),
        field(&report["source_scope"]["description"])
    );
    if let Some(results) = report["results"].as_array() {
        let shown = if limit == 0 {
            results.len()
        } else {
            limit.min(results.len())
        };
        let _ = writeln!(output, "\nShowing {shown} of {} selected or uncertain documents. The display limit does not change retrieval.", results.len());
        for (i, row) in results.iter().take(shown).enumerate() {
            let _ = writeln!(
                output,
                "\n{}. {} [{}; score {}]\n   {}\n   Source: {}\n   {}\n   Text: {}",
                i + 1,
                field(&row["title"]),
                field(&row["status"]),
                row["probability"],
                field(&row["url"]),
                field(&row["source_id"]),
                field(row.get("text").unwrap_or(&row["excerpt"])),
                field(&row["text_path"])
            );
        }
    }
    let _ = writeln!(
        output,
        "\nScores estimate relevance. Check source dates and complete text before use."
    );
    let _ = writeln!(output, "Source responses can contain only summaries or chunks. Read document provenance for content scope.");
    let _ = writeln!(
        output,
        "Reports: {}/failures.json\nJSON: {}\nJev cost: ${}",
        field(&report["directory"]),
        field(&report["report_path"]),
        report["usage"]["cost_usd"]
    );
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_policy_never_inverts_exact_scores_and_banded_breaks_near_ties_by_completeness() {
        use std::cmp::Ordering::*;
        // A 0.984 index excerpt against a 0.976 complete page.
        let excerpt = (Some(0.984), 1, "x");
        let page = (Some(0.976), 4, "y");
        assert_eq!(
            compare_ranked(RankPolicy::Raw, excerpt, page),
            Less,
            "raw keeps the higher score first"
        );
        assert_eq!(
            compare_ranked(RankPolicy::Banded, excerpt, page),
            Greater,
            "banded lets completeness decide inside 0.98"
        );
        for policy in [RankPolicy::Raw, RankPolicy::Banded] {
            // Exact ties fall to completeness, then ID.
            assert_eq!(
                compare_ranked(policy, (Some(0.98), 1, "a"), (Some(0.98), 4, "b")),
                Greater
            );
            assert_eq!(
                compare_ranked(policy, (Some(0.5), 0, "a"), (Some(0.5), 0, "b")),
                Less
            );
            assert_eq!(
                compare_ranked(policy, (None, 0, "a"), (Some(0.1), 0, "b")),
                Greater,
                "missing score sorts last"
            );
        }
    }
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
        };
        write(
            "question.json",
            &json!({"question":"q","config":{"fixture":true}}),
        );
        write("source-scope.json", &json!({"scope":"all"}));
        write("scores.json", &json!([score("a", 0.5), score("b", 0.9)]));
        write("failures.json", &json!([]));
        write("omitted.json", &json!([]));
        write("selected.json", &json!([doc("a"), doc("b")]));
        write("uncertain.json", &json!([]));
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
        let report = build_report_variant(&outcome, false, Some("t"), RankPolicy::Raw).unwrap();
        assert_eq!(report["results"][0]["id"], "b");
        assert_eq!(report["results"][0]["signals"]["usable_evidence"], 0.9);
        assert!(root.join("search-t.json").exists());
        assert!(root.join("search-documents-t").is_dir());
        assert_eq!(
            std::fs::read(root.join("search.json")).unwrap(),
            b"original"
        );
        assert!(
            build_report_variant(&outcome, false, Some("t"), RankPolicy::Raw).is_err(),
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
        let report = build_report_variant(&outcome, false, Some("n"), RankPolicy::Banded).unwrap();
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
        assert!(build_report_variant(&outcome, false, Some("m"), RankPolicy::Banded).is_err());
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
    fn complete_html_article_wins_a_score_tie_against_catalog_metadata() {
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
        for policy in [RankPolicy::Banded, RankPolicy::Raw] {
            assert_eq!(
                compare_ranked(
                    policy,
                    (Some(0.98), scope_rank(&article), &article.id),
                    (Some(0.98), scope_rank(&catalog), &catalog.id),
                ),
                std::cmp::Ordering::Less,
                "The complete article must sort before catalog metadata under {policy:?}"
            );
        }
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
    }

    #[test]
    fn text_output_removes_terminal_controls_without_changing_json_evidence() {
        let malicious = "title\u{1b}]52;c;payload\u{7}\n\u{202e}é";
        let report =
            json!({"question":malicious,"results":[{"title":malicious,"excerpt":malicious}]});
        let rendered = render_text(&report, 0);
        assert!(!rendered.contains('\u{1b}'));
        assert!(!rendered.contains('\u{7}'));
        assert!(!rendered.contains('\u{202e}'));
        assert!(rendered.contains('é'));
        assert_eq!(report["question"], malicious);
    }
}
