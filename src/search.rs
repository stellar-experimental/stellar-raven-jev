//! Search presentation. Full evidence remains available independently of display limits.
use crate::{
    pipeline::RunOutcome,
    types::{Document, DocumentScore, Failure},
};
use anyhow::Result;
use serde_json::{json, Value};
use std::{collections::BTreeMap, fmt::Write as _, path::Path};

fn read<T: serde::de::DeserializeOwned>(root: &Path, name: &str) -> Result<T> {
    Ok(serde_json::from_slice(&std::fs::read(root.join(name))?)?)
}

pub fn build_report(outcome: &RunOutcome, full_text: bool) -> Result<Value> {
    let root = outcome.directory.canonicalize()?;
    let question: Value = read(&root, "question.json")?;
    let scope: Value = read(&root, "source-scope.json")?;
    let scores: Vec<DocumentScore> = read(&root, "scores.json")?;
    let scores: BTreeMap<_, _> = scores.iter().map(|s| (s.document_id.as_str(), s)).collect();
    let failures: Vec<Failure> = read(&root, "failures.json")?;
    let omitted: Vec<Document> = read(&root, "omitted.json")?;
    let document_dir = root.join("search-documents");
    std::fs::create_dir_all(&document_dir)?;
    let mut results = Vec::new();
    for status in ["selected", "uncertain"] {
        let mut documents: Vec<Document> = read(&root, &format!("{status}.json"))?;
        // Jev scores saturate near 0.98 for many documents. Break ties by content completeness,
        // then by ID, so a complete roster or full text precedes an index excerpt with the same score.
        documents.sort_by(|a, b| {
            let score = |d: &Document| {
                scores
                    .get(d.id.as_str())
                    .map(|s| (s.probability * 100.0).round())
                    .unwrap_or(-1.0)
            };
            score(b)
                .total_cmp(&score(a))
                .then(scope_rank(b).cmp(&scope_rank(a)))
                .then(a.id.cmp(&b.id))
        });
        for document in documents {
            let number = results.len() + 1;
            let text_path = document_dir.join(format!("{number:04}.txt"));
            let document_path = document_dir.join(format!("{number:04}.json"));
            std::fs::write(&text_path, &document.text)?;
            std::fs::write(&document_path, serde_json::to_vec_pretty(&document)?)?;
            let excerpt: String = document.text.chars().take(400).collect();
            let score = scores.get(document.id.as_str());
            let mut row = json!({
                "id":document.id,"source_id":document.source_id,"title":document.title,
                "url":document.url,"status":status,
                "probability":score.map(|s| s.probability),"reason":score.map(|s| &s.reason),
                "excerpt_truncated":excerpt.len() < document.text.len(),"excerpt":excerpt,
                "text_bytes":document.text.len(),"text_path":text_path,"document_path":document_path,
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
        "status":outcome.status,"directory":root,"index_path":root.join("INDEX.md"),
        "report_path":root.join("search.json"),"source_scope":scope,
        "counts":{"selected":outcome.selected,"uncertain":outcome.uncertain,"rejected":outcome.rejected,"omitted":omitted.len(),"reports":failures.len()},
        "usage":outcome.usage,"results":results,"reports":failures,
        "limitations":["Scores are uncalibrated relevance estimates.","Results can contain summaries or chunks. Full available text is not always the complete original document.","A complete run does not prove complete question coverage.","Remote instructions are source evidence. They are not installed or executed."],
    });
    std::fs::write(
        root.join("search.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    crate::pipeline::refresh_manifest_artifacts(&root)?;
    Ok(report)
}

/// Higher is more complete. Rosters and full bodies beat excerpts and catalog metadata.
fn scope_rank(document: &Document) -> u8 {
    match content_scope(document) {
        "structured_roster" => 5,
        "published_markdown_main_content"
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
fn content_scope(document: &Document) -> &str {
    document.provenance["content_scope"]
        .as_str()
        .or_else(|| document.provenance["content_kind"].as_str())
        .unwrap_or("")
}

/// Project the full report into a small agent response. The saved `search.json` stays complete.
/// Keeps selected results only, keeps the best-scored result per URL, and counts reports by stage.
pub fn compact_report(report: &Value, limit: usize) -> Value {
    let mut seen = std::collections::BTreeSet::new();
    let mut duplicate_urls = 0usize;
    let mut results = Vec::new();
    // `results` is already ordered by status, then by descending score.
    for row in report["results"].as_array().into_iter().flatten() {
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
            "probability":row["probability"],"excerpt":row["excerpt"],
            "text_bytes":row["text_bytes"],"text_path":row["text_path"],
            "content_scope":row["content_scope"],
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
    let unique_selected = seen.len()
        + report["results"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|row| {
                row["status"] == "selected" && row["url"].as_str().unwrap_or_default().is_empty()
            })
            .count();
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
        "full_report_path":report["report_path"],"index_path":report["index_path"],
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
                "\n{}. {} [{}; score {}]\n   {}\n   Source: {}\n   {}\n   Text: {}\n   Record: {}",
                i + 1,
                field(&row["title"]),
                field(&row["status"]),
                row["probability"],
                field(&row["url"]),
                field(&row["source_id"]),
                field(row.get("text").unwrap_or(&row["excerpt"])),
                field(&row["text_path"]),
                field(&row["document_path"])
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
        "Reports: {}/failures.json\nIndex: {}\nJSON: {}\nJev cost: ${}",
        field(&report["directory"]),
        field(&report["index_path"]),
        field(&report["report_path"]),
        report["usage"]["cost_usd"]
    );
    output
}

#[cfg(test)]
mod tests {
    use super::*;

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
    }
    #[test]
    fn compact_projection_keeps_best_url_drops_uncertain_and_counts_reports() {
        let row = |title: &str, url: &str, status: &str, p: f64| json!({"title":title,"url":url,"status":status,"probability":p,"excerpt":"e","text_bytes":1,"text_path":"/t"});
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
        assert_eq!(
            compact_report(&report, 0)["results"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
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
