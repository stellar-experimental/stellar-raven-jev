//! Human-readable exports preserve source text without generating an answer.
use crate::types::{Document, DocumentScore, Failure};
use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    io::ErrorKind,
    path::Path,
};

fn read_optional<T: DeserializeOwned>(
    root: &Path,
    name: &str,
    missing: &mut Vec<String>,
) -> Result<Option<T>> {
    match std::fs::read(root.join(name)) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .with_context(|| format!("Invalid artifact {name}"))
            .map(Some),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            missing.push(name.into());
            Ok(None)
        }
        Err(error) => Err(error).with_context(|| format!("Cannot read {name}")),
    }
}

fn ids(root: &Path, name: &str, missing: &mut Vec<String>) -> Result<BTreeSet<String>> {
    Ok(read_optional::<Vec<Document>>(root, name, missing)?
        .unwrap_or_default()
        .into_iter()
        .map(|document| document.id)
        .collect())
}

fn label(text: &str) -> String {
    text.replace(['\n', '\r'], " ")
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('\\', "\\\\")
        .replace('[', "\\[")
        .replace(']', "\\]")
        .replace('*', "\\*")
        .replace('_', "\\_")
        .replace('`', "\\`")
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReportClass {
    Error,
    Warning,
    Unclassified,
}

// Match complete stage names, not substrings or message text. Mixed-purpose stages
// such as upstream, freshness, original, and provenance remain unclassified.
fn report_class(stage: &str) -> ReportClass {
    match stage {
        "initialization"
        | "registry"
        | "route"
        | "fetch"
        | "document_score"
        | "authentication"
        | "http"
        | "parse"
        | "search"
        | "record"
        | "content"
        | "pagination"
        | "lumenloop.auth"
        | "lumenloop.search"
        | "lumenloop.search_shape"
        | "lumenloop.detail"
        | "lumenloop.record"
        | "lumenloop.pagination" => ReportClass::Error,
        "coverage"
        | "limit"
        | "truncation"
        | "document_limit"
        | "source_limit"
        | "query_limit"
        | "query_facet_limit"
        | "query_variant"
        | "query_relaxation"
        | "query_plan"
        | "lumenloop.coverage"
        | "lumenloop.limit"
        | "lumenloop.truncation"
        | "lumenloop.search_limit" => ReportClass::Warning,
        _ => ReportClass::Unclassified,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    Selected,
    Uncertain,
    Rejected,
    Omitted,
    Unclassified,
}
impl Status {
    const ALL: [Self; 5] = [
        Self::Selected,
        Self::Uncertain,
        Self::Rejected,
        Self::Omitted,
        Self::Unclassified,
    ];
    fn name(self) -> &'static str {
        match self {
            Self::Selected => "selected",
            Self::Uncertain => "uncertain",
            Self::Rejected => "rejected",
            Self::Omitted => "omitted from scoring",
            Self::Unclassified => "unclassified",
        }
    }
    fn heading(self) -> &'static str {
        match self {
            Self::Selected => "Selected",
            Self::Uncertain => "Uncertain",
            Self::Rejected => "Rejected",
            Self::Omitted => "Omitted from scoring",
            Self::Unclassified => "Unclassified documents",
        }
    }
}

fn score_text(
    document: &Document,
    status: Status,
    scores: &BTreeMap<&str, Vec<&DocumentScore>>,
) -> String {
    if status == Status::Omitted {
        return "Not applicable: omitted from scoring.".into();
    }
    match scores.get(document.id.as_str()).map(Vec::as_slice) {
        Some([score])
            if score.probability.is_finite() && (0.0..=1.0).contains(&score.probability) =>
        {
            format!("{} — {}", score.probability, label(&score.reason))
        }
        Some([_]) => "Unavailable: the saved probability is invalid. Review scores.json.".into(),
        Some(records) => format!(
            "Ambiguous: {} saved score records share this ID. Review scores.json.",
            records.len()
        ),
        None => "Unavailable: no saved score exists. This is not a zero score.".into(),
    }
}

fn content_scope(document: &Document) -> String {
    let provenance = &document.provenance;
    let kinds: Vec<_> = ["content_scope", "content_kind", "kind"]
        .iter()
        .filter_map(|key| provenance.get(*key).and_then(Value::as_str))
        .collect();
    let partial = kinds.iter().any(|kind| {
        matches!(
            *kind,
            "summary"
                | "ai_summary"
                | "research_chunk"
                | "transcript_excerpt"
                | "index_record"
                | "structured_record"
        )
    });
    let known_incomplete = ["full_original", "full_source_body_available"]
        .iter()
        .any(|key| provenance.get(*key).and_then(Value::as_bool) == Some(false));
    let known_full = ["full_original", "full_source_body_available"]
        .iter()
        .any(|key| provenance.get(*key).and_then(Value::as_bool) == Some(true));
    let description = if document.provenance["fixture"] == true || kinds.contains(&"fixture") {
        "Offline fixture content; not live source evidence."
    } else if partial || known_incomplete {
        "Upstream summary, chunk, excerpt, or record; do not assume the complete original document."
    } else if known_full {
        "The source reports a full body. This export does not independently verify completeness."
    } else {
        "Original-document completeness is unknown. A full API response can still contain only a summary or chunk."
    };
    if kinds.is_empty() {
        description.into()
    } else {
        format!(
            "{} Reported content type: {}.",
            description,
            kinds
                .iter()
                .map(|kind| label(kind))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

fn write_reports(index: &mut String, failures: Option<&[Failure]>) -> Result<()> {
    index.push_str("## Errors and coverage reports\n\n");
    index.push_str("Only explicit coverage/advisory stages count as warnings. Unknown and mixed-purpose stages remain unclassified for review.\n\n");
    let Some(failures) = failures else {
        index.push_str("The failure log is unavailable. Error and warning counts are unknown.\n\n");
        return Ok(());
    };
    let classes = [
        (ReportClass::Error, "Errors"),
        (ReportClass::Warning, "Coverage/advisory warnings"),
        (
            ReportClass::Unclassified,
            "Unclassified reports — review required",
        ),
    ];
    for (class, heading) in classes {
        let reports: Vec<_> = failures
            .iter()
            .filter(|failure| report_class(&failure.stage) == class)
            .collect();
        writeln!(index, "### {heading} ({})\n", reports.len())?;
        if reports.is_empty() {
            index.push_str("None recorded in the available log.\n\n");
        }
        for report in reports {
            writeln!(
                index,
                "- Stage: {}; source: {}. {}",
                label(&report.stage),
                label(report.source_id.as_deref().unwrap_or("run")),
                label(&report.message)
            )?;
        }
        index.push('\n');
    }
    index.push_str("This presentation does not change failures.json or the run status.\n\n");
    Ok(())
}

pub fn write_run_index(root: &Path) -> Result<()> {
    let mut missing = Vec::new();
    let question = read_optional::<Value>(root, "question.json", &mut missing)?;
    let selected = ids(root, "selected.json", &mut missing)?;
    let uncertain = ids(root, "uncertain.json", &mut missing)?;
    let rejected = ids(root, "rejected.json", &mut missing)?;
    let mut documents =
        read_optional::<Vec<Document>>(root, "documents.json", &mut missing)?.unwrap_or_default();
    let admitted = documents.len();
    documents.extend(
        read_optional::<Vec<Document>>(root, "omitted.json", &mut missing)?.unwrap_or_default(),
    );
    let saved_scores =
        read_optional::<Vec<DocumentScore>>(root, "scores.json", &mut missing)?.unwrap_or_default();
    let failures = read_optional::<Vec<Failure>>(root, "failures.json", &mut missing)?;
    let mut scores: BTreeMap<&str, Vec<&DocumentScore>> = BTreeMap::new();
    for score in &saved_scores {
        scores.entry(&score.document_id).or_default().push(score);
    }
    let statuses: Vec<_> = documents
        .iter()
        .enumerate()
        .map(|(number, document)| {
            if number >= admitted {
                return Status::Omitted;
            }
            let matches = [
                selected.contains(&document.id),
                uncertain.contains(&document.id),
                rejected.contains(&document.id),
            ];
            match matches {
                [true, false, false] => Status::Selected,
                [false, true, false] => Status::Uncertain,
                [false, false, true] => Status::Rejected,
                _ => Status::Unclassified,
            }
        })
        .collect();

    // Exact UTF-8 string equality: never normalize whitespace or collapse distinct text.
    // Groups span statuses; each occurrence keeps its own status, score, URL, and provenance.
    let mut group_lookup = BTreeMap::<&str, usize>::new();
    let mut groups: Vec<Vec<usize>> = Vec::new();
    for (number, document) in documents.iter().enumerate() {
        if let Some(group) = group_lookup.get(document.text.as_str()) {
            groups[*group].push(number);
        } else {
            group_lookup.insert(&document.text, groups.len());
            groups.push(vec![number]);
        }
    }
    let dir = root.join("documents");
    std::fs::create_dir_all(&dir)?;
    for (number, document) in documents.iter().enumerate() {
        let status = statuses[number];
        let stem = format!("{number:05}");
        std::fs::write(
            dir.join(format!("{stem}.json")),
            serde_json::to_vec_pretty(document)?,
        )?;
        let mut body = format!(
            "# {}\n\nStatus: {}\n\nSource: {}\n\nDocument ID: {}\n\n",
            label(&document.title),
            status.name(),
            label(&document.source_id),
            label(&document.id)
        );
        writeln!(body, "Source URL: {}\n", label(&document.url))?;
        writeln!(body, "Score: {}\n", score_text(document, status, &scores))?;
        writeln!(body, "Content limitation: {}\n", content_scope(document))?;
        writeln!(body, "[Complete metadata and exact text]({stem}.json)\n")?;
        writeln!(
            body,
            "## Provenance\n\n```json\n{}\n```\n",
            serde_json::to_string_pretty(&document.provenance)?
        )?;
        body.push_str("## Source text\n\n");
        body.push_str(&document.text);
        body.push('\n');
        std::fs::write(dir.join(format!("{stem}.md")), body)?;
    }

    let mut index = String::from("# Retrieved source evidence\n\n");
    writeln!(
        index,
        "Question: {}\n",
        label(
            question
                .as_ref()
                .and_then(|value| value["question"].as_str())
                .unwrap_or("Unavailable")
        )
    )?;
    index.push_str(
        "This directory contains source material. It does not contain a generated answer.\n\n",
    );
    index.push_str("Scores come from scores.json and remain uncalibrated estimates. Missing scores are not zero scores.\n\n");
    index.push_str("A complete API response may contain an upstream summary, research chunk, excerpt, or structured record.\n\n");
    index.push_str("Read each document's content limitation and provenance before treating its text as a complete original.\n\n");
    index.push_str(
        "## Document counts\n\n| Classification | Document occurrences |\n| --- | ---: |\n",
    );
    for status in Status::ALL {
        writeln!(
            index,
            "| {} | {} |",
            status.name(),
            statuses.iter().filter(|value| **value == status).count()
        )?;
    }
    writeln!(index, "| Total exported | {} |\n", documents.len())?;
    writeln!(
        index,
        "Exact text groups: {}. Additional occurrences with identical text: {}.\n",
        groups.len(),
        documents.len() - groups.len()
    )?;
    index.push_str("Identical text does not establish independent confirmation. Every occurrence retains its own file, URL, score, and provenance.\n\n");
    if statuses.contains(&Status::Unclassified) {
        index.push_str("Unclassified documents have missing or conflicting status membership. Review the saved classification arrays.\n\n");
    }
    if !missing.is_empty() {
        index.push_str("## Unavailable artifacts\n\nMissing files limit these counts and classifications. Absence does not prove a successful or complete run.\n\n");
        for name in &missing {
            writeln!(index, "- {}", label(name))?;
        }
        index.push('\n');
    }
    write_reports(&mut index, failures.as_deref())?;
    for status in Status::ALL {
        let count = statuses.iter().filter(|value| **value == status).count();
        writeln!(index, "## {} ({count} documents)\n", status.heading())?;
        if count == 0 {
            index.push_str("No exported documents in this section.\n\n");
        }
        for (group, members) in groups.iter().enumerate() {
            let in_section: Vec<_> = members
                .iter()
                .copied()
                .filter(|number| statuses[*number] == status)
                .collect();
            if in_section.is_empty() {
                continue;
            }
            writeln!(
                index,
                "### Text group {group:05} ({} in this section)\n",
                in_section.len()
            )?;
            if members.len() > 1 {
                writeln!(index, "Exact duplicate text: {} occurrences across this run. Each keeps its original classification.\n", members.len())?;
            }
            for number in in_section {
                let document = &documents[number];
                writeln!(
                    index,
                    "#### [{}](documents/{number:05}.md)\n",
                    label(&document.title)
                )?;
                writeln!(
                    index,
                    "Source: {}. Document ID: {}.\n",
                    label(&document.source_id),
                    label(&document.id)
                )?;
                writeln!(index, "Source URL: {}\n", label(&document.url))?;
                writeln!(index, "Score: {}\n", score_text(document, status, &scores))?;
                writeln!(index, "Content limitation: {}\n", content_scope(document))?;
                writeln!(
                    index,
                    "[Exact text, URL, and provenance](documents/{number:05}.json)\n"
                )?;
            }
        }
    }
    index.push_str("## Saved evidence\n\n");
    for name in [
        "question.json",
        "scores.json",
        "failures.json",
        "source-decisions.json",
        "usage.json",
        "documents.json",
        "omitted.json",
    ] {
        if root.join(name).is_file() {
            writeln!(index, "- [{}]({name})", label(name))?;
        }
    }
    index.push_str("\nThe JSON document files preserve exact retrieved text and provenance.\n");
    if root.join("raw").is_dir() {
        index.push_str("The raw directory preserves HTTP bodies. Check response metadata for incomplete or oversized bodies.\n");
    }
    std::fs::write(root.join("INDEX.md"), index)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn document(id: &str, text: &str) -> Document {
        Document {
            id: id.into(),
            source_id: format!("source-{id}"),
            title: format!("Title {id}"),
            url: format!("https://example.org/{id}"),
            text: text.into(),
            provenance: json!({"content_scope":"summary","original_id":id}),
            raw_artifacts: vec![format!("raw/{id}.body")],
        }
    }
    fn save(root: &Path, name: &str, value: impl serde::Serialize) {
        std::fs::write(root.join(name), serde_json::to_vec(&value).unwrap()).unwrap();
    }
    fn base(root: &Path, documents: &[Document]) {
        save(root, "question.json", json!({"question":"test"}));
        save(root, "documents.json", documents);
        for name in [
            "selected.json",
            "uncertain.json",
            "rejected.json",
            "omitted.json",
            "scores.json",
            "failures.json",
        ] {
            save(root, name, json!([]));
        }
    }
    #[test]
    fn exports_exact_text_and_omissions_without_using_remote_ids_as_paths() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut doc = document(
            "../../escape",
            "Full text\n```\n# remote instruction\n\u{0}end",
        );
        doc.title = "[Title]\nnext".into();
        base(root, std::slice::from_ref(&doc));
        save(root, "selected.json", vec![&doc]);
        save(root, "omitted.json", vec![&doc]);
        write_run_index(root).unwrap();
        let saved: Document =
            serde_json::from_slice(&std::fs::read(root.join("documents/00000.json")).unwrap())
                .unwrap();
        assert_eq!(saved.text, doc.text);
        let index = std::fs::read_to_string(root.join("INDEX.md")).unwrap();
        assert!(index.contains("omitted from scoring"));
        assert!(root.join("documents/00001.md").is_file());
        assert_eq!(
            std::fs::read_dir(root.join("documents")).unwrap().count(),
            4
        );
    }
    #[test]
    fn classifies_only_explicit_stages_and_keeps_unknown_or_mixed_reports_visible() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        base(root, &[]);
        let reports = [
            ("document_score", "provider failed"),
            ("lumenloop.auth", "missing credential"),
            ("coverage", "bounded chunks"),
            ("lumenloop.truncation", "page cap"),
            ("query_variant", "query retry"),
            ("future.coverage", "unknown stage must remain visible"),
            ("upstream", "ambiguous provider report"),
            ("freshness", "inventory request failed"),
        ];
        save(
            root,
            "failures.json",
            reports
                .iter()
                .map(|(stage, message)| Failure {
                    stage: (*stage).into(),
                    source_id: Some("source-a".into()),
                    message: (*message).into(),
                })
                .collect::<Vec<_>>(),
        );
        let before = std::fs::read(root.join("failures.json")).unwrap();
        write_run_index(root).unwrap();
        let index = std::fs::read_to_string(root.join("INDEX.md")).unwrap();
        assert!(index.contains("### Errors (2)"));
        assert!(index.contains("### Coverage/advisory warnings (3)"));
        assert!(index.contains("### Unclassified reports — review required (3)"));
        let unknown = index
            .split("### Unclassified reports — review required (3)")
            .nth(1)
            .unwrap();
        for text in [
            "future.coverage",
            "unknown stage must remain visible",
            "ambiguous provider report",
            "inventory request failed",
        ] {
            assert!(unknown.contains(text));
        }
        assert_eq!(std::fs::read(root.join("failures.json")).unwrap(), before);
    }
    #[test]
    fn groups_exact_duplicates_and_retains_every_url_provenance_status_and_score() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let docs = vec![
            document("a", "same full text"),
            document("b", "same full text"),
            document("c", "same full text"),
            document("d", "same full text\n"),
        ];
        base(root, &docs);
        save(root, "selected.json", vec![&docs[0], &docs[1]]);
        save(root, "uncertain.json", vec![&docs[2]]);
        save(root, "rejected.json", vec![&docs[3]]);
        save(
            root,
            "scores.json",
            vec![
                DocumentScore {
                    document_id: "a".into(),
                    probability: 0.98765,
                    reason: "saved reason".into(),
                },
                DocumentScore {
                    document_id: "b".into(),
                    probability: 0.76,
                    reason: "other saved reason".into(),
                },
            ],
        );
        let snapshots: Vec<_> = [
            "documents.json",
            "selected.json",
            "uncertain.json",
            "rejected.json",
            "scores.json",
        ]
        .iter()
        .map(|name| (*name, std::fs::read(root.join(name)).unwrap()))
        .collect();
        write_run_index(root).unwrap();
        let index = std::fs::read_to_string(root.join("INDEX.md")).unwrap();
        assert!(
            index.contains("Exact text groups: 2. Additional occurrences with identical text: 2.")
        );
        assert!(index.contains("## Selected (2 documents)"));
        assert!(index.contains("## Uncertain (1 documents)"));
        assert!(index.contains("## Rejected (1 documents)"));
        assert!(index.contains("### Text group 00000 (2 in this section)"));
        assert!(index.contains("### Text group 00000 (1 in this section)"));
        assert!(index.contains("Score: 0.98765 — saved reason"));
        assert!(index.contains("Score: 0.76 — other saved reason"));
        assert!(index.contains("no saved score exists"));
        assert!(index.contains("Upstream summary, chunk, excerpt, or record"));
        for (number, doc) in docs.iter().enumerate() {
            let saved: Document = serde_json::from_slice(
                &std::fs::read(root.join(format!("documents/{number:05}.json"))).unwrap(),
            )
            .unwrap();
            assert_eq!(saved.url, doc.url);
            assert_eq!(saved.provenance, doc.provenance);
            assert_eq!(saved.raw_artifacts, doc.raw_artifacts);
            assert_eq!(saved.text, doc.text);
            assert!(index.contains(&doc.url));
            assert!(root.join(format!("documents/{number:05}.md")).is_file());
        }
        for (name, before) in snapshots {
            assert_eq!(std::fs::read(root.join(name)).unwrap(), before);
        }
    }
    #[test]
    fn minimal_artifacts_report_missing_inputs_without_inventing_success_or_scores() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        save(root, "documents.json", vec![document("a", "text")]);
        write_run_index(root).unwrap();
        let index = std::fs::read_to_string(root.join("INDEX.md")).unwrap();
        assert!(index.contains("Question: Unavailable"));
        assert!(index.contains("## Unavailable artifacts"));
        assert!(index.contains("Error and warning counts are unknown"));
        assert!(index.contains("## Unclassified documents (1 documents)"));
        assert!(!index.contains("[failures.json](failures.json)"));
        assert!(!index.contains("Score: 0"));
        assert!(index.contains("scores.json"));
    }
    #[test]
    fn malformed_existing_artifacts_fail_instead_of_looking_missing() {
        let dir = tempfile::tempdir().unwrap();
        base(dir.path(), &[]);
        std::fs::write(dir.path().join("scores.json"), "not JSON").unwrap();
        assert!(write_run_index(dir.path())
            .unwrap_err()
            .to_string()
            .contains("Invalid artifact scores.json"));
        assert!(!dir.path().join("INDEX.md").exists());
    }
    #[test]
    fn conflicting_statuses_and_duplicate_score_ids_are_explicit() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let doc = document("a", "text");
        base(root, std::slice::from_ref(&doc));
        save(root, "selected.json", vec![&doc]);
        save(root, "rejected.json", vec![&doc]);
        let score = DocumentScore {
            document_id: "a".into(),
            probability: 0.9,
            reason: "test".into(),
        };
        save(root, "scores.json", vec![&score, &score]);
        write_run_index(root).unwrap();
        let index = std::fs::read_to_string(root.join("INDEX.md")).unwrap();
        assert!(index.contains("missing or conflicting status membership"));
        assert!(index.contains("Ambiguous: 2 saved score records share this ID"));
        assert!(!index.contains("Score: 0.9"));
    }
}
