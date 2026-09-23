//! Bounded LumenLoop reads. External access exposes summaries, published research,
//! and SCF applications; it does not expose article bodies or AV transcripts.
use crate::types::*;
use anyhow::{bail, Result};
use reqwest::Method;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

const BASE: &str = "https://api.lumenloop.com/v1/tools";
const WINDOW: usize = 20;
const COLLECTIONS: &[(&str, &str, &str)] = &[
    ("articles", "Articles", "News and articles about Stellar; AI summaries and original URLs, without full article bodies."),
    ("av", "Audio and video", "Stellar talks, podcasts, and videos; AI summaries and recording URLs, without transcripts."),
    ("av_passages", "AV passage discovery", "Find talks by transcript passages; external access returns AI summaries and opaque text offsets, never transcript quotations or playback timestamps."),
    ("jobs", "Jobs", "Active Stellar job listings searched by title or URL. Catalog metadata and observed status only; full descriptions and publisher status are unavailable."),
    ("events", "Events", "Stellar event metadata, dates, locations, AI summaries, and source links."),
    ("research", "Published research", "LumenLoop's published editorial research; stored full bodies where available."),
    ("proposals", "Governance proposals", "Stellar ecosystem governance proposals; AI summaries, vote metadata, and source links."),
    ("directory", "Project directory", "Stellar projects, categories, descriptions, public links, and project metadata."),
    ("scf", "SCF applications", "Stellar Community Fund applications, award metadata, and stored application bodies where available."),
];

pub fn sources() -> Vec<Source> {
    COLLECTIONS
        .iter()
        .map(|(kind, name, description)| Source {
            id: format!("lumenloop.{kind}"),
            name: format!("LumenLoop {name}"),
            description: format!("{description} Covered tools: {}. Other LumenLoop read tools are deferred; see docs/research/lumenloop.md.", match *kind {
                "jobs" => "list_documents (active keyword search and pagination)",
                "directory" => "search_directory, get_project",
                "scf" => "find_similar_scf_submissions, get_scf_submissions, get_project",
                "av_passages" => "find_av_passages, get_document",
                _ => "search_content_semantic, get_document",
            }),
            family: "lumenloop".into(),
        })
        .collect()
}

/// Detail reads add text only where discovery returns partial records.
/// `search_content_semantic` with `response_format: detailed` already returns the complete stored
/// summary for these collections: 300 of 300 detail reads across eight live runs on 2026-09-21
/// returned identical title, URL, and text. Skipping them avoids the provider's per-minute quota.
fn needs_detail(kind: &str) -> bool {
    !matches!(
        kind,
        "articles" | "av" | "av_passages" | "events" | "proposals"
    )
}

fn failure(source: &Source, stage: &str, message: impl Into<String>) -> Failure {
    Failure {
        stage: format!("lumenloop.{stage}"),
        source_id: Some(source.id.clone()),
        message: message.into(),
    }
}

#[derive(Clone)]
struct Reply {
    data: Value,
    artifact: String,
}
#[derive(Clone, Debug)]
struct ReadError {
    message: String,
    stop: bool,
    artifact: Option<String>,
}

fn decode(status: u16, value: Value, artifact: &str) -> std::result::Result<Value, ReadError> {
    let stop = matches!(status, 401 | 402 | 403 | 429)
        || matches!(
            value["code"].as_str(),
            Some("unauthorized" | "insufficient_scope" | "payment_required" | "rate_limited")
        );
    if !(200..300).contains(&status) || value["success"] != true {
        return Err(ReadError {
            message: format!(
                "HTTP {status}; API code {}; response: {artifact}",
                value["code"].as_str().unwrap_or("unspecified")
            ),
            stop,
            artifact: Some(artifact.into()),
        });
    }
    let mut data = value.get("data").cloned().unwrap_or(Value::Null);
    if value["meta"]["format"] == "blocks" {
        if let Some(blocks) = data["content"].as_array() {
            if blocks.len() == 1 {
                if let Some(text) = blocks[0]["text"].as_str() {
                    data = serde_json::from_str(text).unwrap_or_else(|_| json!({"text": text}));
                }
            }
        }
    }
    if data.get("text").is_some()
        || data.get("content").is_some_and(Value::is_array)
        || data.is_null()
    {
        return Err(ReadError { message: format!("The tool returned a message or unsupported content instead of records; response: {artifact}"), stop: false, artifact: Some(artifact.into()) });
    }
    Ok(data)
}

async fn read(
    ctx: &FetchContext,
    key: &str,
    tool: &str,
    args: Value,
) -> std::result::Result<Reply, ReadError> {
    // Tool names are constants selected below. The question never selects an operation or URL.
    let response = ctx
        .http
        .request(
            Method::POST,
            &format!("{BASE}/{tool}"),
            vec![
                ("Authorization".into(), format!("Bearer {key}")),
                ("Accept".into(), "application/json".into()),
            ],
            Some(args),
        )
        .await
        .map_err(|error| ReadError {
            message: error.to_string(),
            stop: false,
            artifact: None,
        })?;
    let envelope = response.json().map_err(|_| ReadError {
        message: format!("Invalid JSON; response: {}", response.artifact),
        stop: matches!(response.status, 401 | 402 | 403 | 429),
        artifact: Some(response.artifact.clone()),
    })?;
    let data = decode(response.status, envelope, &response.artifact)?;
    Ok(Reply {
        data,
        artifact: response.artifact,
    })
}

fn identifier(row: &Value, kind: &str) -> Option<String> {
    let key = if matches!(kind, "directory" | "scf") {
        "slug"
    } else {
        "id"
    };
    let value = if kind == "av_passages" {
        row.get("av_id").or_else(|| row.get("id"))?
    } else {
        row.get(key)?
    };
    match value {
        Value::String(value) if !value.is_empty() => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn numeric_id(row: &Value) -> Option<u64> {
    let row = if row.get("av_id").is_some() {
        json!({"id":row["av_id"]})
    } else {
        row.clone()
    };
    let id = row["id"]
        .as_u64()
        .or_else(|| row["id"].as_str()?.parse::<u64>().ok())?;
    // The upstream schema uses a JavaScript number. Do not round large database IDs.
    (id <= 9_007_199_254_740_991).then_some(id)
}

fn record_text(row: &Value, kind: &str) -> (String, &'static str, bool) {
    for field in ["body", "application", "content"] {
        if let Some(body) = row[field].as_str().filter(|body| !body.is_empty()) {
            return (
                body.to_owned(),
                match field {
                    "body" => "stored_editorial_body",
                    "application" => "stored_application_body",
                    _ => "stored_source_body",
                },
                true,
            );
        }
    }
    if let Some(chunk) = row["chunk_text"].as_str().filter(|text| !text.is_empty()) {
        return (chunk.into(), "transcript_excerpt", false);
    }
    if matches!(kind, "directory" | "jobs") {
        return (
            serde_json::to_string_pretty(row).unwrap_or_default(),
            if kind == "jobs" {
                "job_catalog_metadata"
            } else {
                "project_metadata"
            },
            false,
        );
    }
    let mut text = Vec::new();
    for field in [
        "title",
        "summary",
        "long_summary",
        "description",
        "body_preview",
    ] {
        if let Some(value) = row[field].as_str().filter(|value| !value.is_empty()) {
            if !text.iter().any(|item: &String| item == value) {
                text.push(value.to_owned());
            }
        }
    }
    let summary = row["summary"].is_string() || row["long_summary"].is_string();
    let kind = if summary {
        "ai_summary"
    } else {
        "catalog_metadata"
    };
    // Preserve structured facts when the record has no prose fields.
    if text.is_empty() {
        text.push(serde_json::to_string_pretty(row).unwrap_or_default());
    }
    (text.join("\n\n"), kind, false)
}

fn record_url(row: &Value, kind: &str) -> String {
    for field in ["url", "submission_url"] {
        if let Some(url) = row[field].as_str().filter(|url| !url.is_empty()) {
            return url.into();
        }
    }
    if kind == "directory" {
        if let Some(url) = row["links"]["website"]
            .as_array()
            .and_then(|values| values.first())
            .and_then(Value::as_str)
        {
            return url.into(); // Preserve the supplied value, including schemeless domains.
        }
        if let Some(website) = row["website"].as_str() {
            if let Ok(Value::Array(urls)) = serde_json::from_str::<Value>(website) {
                return urls.first().and_then(Value::as_str).unwrap_or("").into();
            }
            return website.into();
        }
    }
    String::new()
}

fn make_document(
    source: &Source,
    kind: &str,
    row: &Value,
    reply: &Reply,
    meta: &Value,
) -> Option<Document> {
    let id = identifier(row, kind)?;
    let (text, content_kind, full_body) = record_text(row, kind);
    Some(Document {
        id: format!("{}:{id}", source.id),
        source_id: source.id.clone(),
        title: row["title"].as_str().unwrap_or("").into(),
        url: record_url(row, kind),
        text,
        provenance: json!({
            "provider":"lumenloop", "collection":kind, "record_id":id,
            "discovery":row, "search_metadata":meta, "detail_status":"not_requested",
            "content_kind":content_kind, "full_source_body_available":full_body,
            "body_completeness":"stored_content_only; publisher parity is unverified",
            "pagination":{"mode":"top_k", "continuation_supported":false},
        }),
        raw_artifacts: vec![reply.artifact.clone()],
    })
}

fn apply_detail(document: &mut Document, row: &Value, kind: &str, artifact: &str) {
    let mut merged = document.provenance["discovery"].clone();
    if let (Some(base), Some(fields)) = (merged.as_object_mut(), row.as_object()) {
        for (key, value) in fields {
            base.insert(key.clone(), value.clone());
        }
    }
    let (text, content_kind, full_body) = record_text(&merged, kind);
    document.title = merged["title"].as_str().unwrap_or(&document.title).into();
    document.url = record_url(&merged, kind);
    document.text = text;
    document.provenance["detail"] = row.clone();
    document.provenance["detail_status"] = json!("retrieved");
    document.provenance["content_kind"] = json!(content_kind);
    document.provenance["full_source_body_available"] = json!(full_body);
    if !document.raw_artifacts.iter().any(|path| path == artifact) {
        document.raw_artifacts.push(artifact.into());
    }
}

fn fixture(source: &Source, kind: &str) -> FetchResult {
    let row = match kind {
        "directory" => {
            json!({"slug":"fixture-project", "title":"Fixture Stellar project", "description":"Offline fixture: Stellar smart contracts and payments.", "links":{"website":["https://example.invalid/lumenloop/project"]}})
        }
        "scf" => {
            json!({"slug":"fixture-submission", "title":"Fixture SCF application", "application":"Offline fixture application: Build developer tools for Soroban smart contracts.", "submission_url":"https://example.invalid/lumenloop/scf"})
        }
        "research" => {
            json!({"id":1, "title":"Fixture published research", "body":"Offline fixture editorial: Stellar smart contracts support application development.", "url":"https://example.invalid/lumenloop/research"})
        }
        _ => {
            json!({"id":"1", "title":format!("Fixture {kind}"), "summary":"Offline fixture summary: Stellar smart contracts and ecosystem development.", "url":format!("https://example.invalid/lumenloop/{kind}")})
        }
    };
    let reply = Reply {
        data: Value::Null,
        artifact: String::new(),
    };
    let mut doc = make_document(source, kind, &row, &reply, &json!({"fixture":true})).unwrap();
    doc.raw_artifacts.clear();
    doc.provenance["fixture"] = json!(true);
    FetchResult {
        documents: vec![doc],
        failures: vec![],
    }
}

fn append_rows(
    source: &Source,
    kind: &str,
    rows: &[Value],
    reply: &Reply,
    meta: &Value,
    limit: usize,
    result: &mut FetchResult,
) {
    let mut seen: HashSet<String> = result.documents.iter().map(|doc| doc.id.clone()).collect();
    let mut duplicate_count = 0;
    for row in rows.iter().take(limit) {
        match make_document(source, kind, row, reply, meta) {
            Some(mut document) if seen.insert(document.id.clone()) => {
                document.provenance["pagination"]["requested_limit"] = json!(limit);
                if kind == "av_passages" {
                    document.provenance["parent_collection"] = json!("av");
                    document.provenance["offset_unit"] =
                        json!("transcript_text_offset_not_playback_seconds");
                }
                result.documents.push(document);
            }
            Some(document) => {
                duplicate_count += 1;
                if let Some(existing) = result
                    .documents
                    .iter_mut()
                    .find(|item| item.id == document.id)
                {
                    if !existing.raw_artifacts.contains(&reply.artifact) {
                        existing.raw_artifacts.push(reply.artifact.clone());
                    }
                    if existing.provenance.get("duplicate_records").is_none() {
                        existing.provenance["duplicate_records"] = json!([]);
                    }
                    if let Some(records) = existing.provenance["duplicate_records"].as_array_mut() {
                        records.push(json!({"row":row,"artifact":reply.artifact}));
                    }
                }
            }
            None => result.failures.push(failure(
                source,
                "record",
                format!(
                    "A search record has no usable ID; response: {}",
                    reply.artifact
                ),
            )),
        }
    }
    if duplicate_count > 0 {
        result.failures.push(failure(
            source,
            "duplicates",
            format!(
                "Skipped {duplicate_count} duplicate IDs; raw records remain in {}.",
                reply.artifact
            ),
        ));
    }
}

/// Substring queries for the jobs title and URL search, one per request: every explicit quoted
/// phrase, else the question's names and identifiers, then its other content words, in question
/// order. A quoted span longer than the planner's data limit is data, not a phrase. The
/// collection's own nouns are dropped. An empty query lists all active jobs.
fn jobs_queries(question: &str) -> Vec<String> {
    let parts: Vec<&str> = question.split('"').collect();
    // Odd parts are inside quotes; the last one is unclosed when the part count is even.
    let closed = if parts.len() % 2 == 1 {
        parts.len()
    } else {
        parts.len() - 1
    };
    let quoted: Vec<String> = parts[..closed]
        .iter()
        .skip(1)
        .step_by(2)
        .map(|text| text.trim())
        .filter(|text| {
            !text.is_empty() && text.split_whitespace().count() <= crate::query::QUOTED_DATA_TOKENS
        })
        .map(|text| text.to_owned())
        .collect();
    if !quoted.is_empty() {
        return quoted;
    }
    let plan = crate::query::plan(question);
    let content = plan
        .keyword()
        .into_iter()
        .filter(|variant| variant.kind == crate::query::VariantKind::Keywords)
        .flat_map(|variant| {
            variant
                .text
                .split_whitespace()
                .map(|word| word.trim_matches('"').to_owned())
                .collect::<Vec<_>>()
        });
    let mut queries: Vec<String> = Vec::new();
    for word in plan.entities.iter().cloned().chain(content) {
        let lower = word.to_lowercase();
        if word.is_empty() || matches!(lower.as_str(), "job" | "jobs") {
            continue;
        }
        if !queries
            .iter()
            .any(|query| query.eq_ignore_ascii_case(&word))
        {
            queries.push(word);
        }
    }
    if queries.is_empty() {
        queries.push(String::new());
    }
    queries
}

fn jobs_args(query: &str, page: usize, limit: usize) -> Value {
    json!({"collection":"jobs","search":query,"status":"active","sort":"published_at","order":"DESC","page":page,"limit":limit})
}

async fn fetch_jobs(ctx: &FetchContext, source: &Source, question: &str, key: &str) -> FetchResult {
    let mut result = FetchResult::default();
    let queries = jobs_queries(question);
    // Keep the page size fixed: changing it changes offset calculations upstream.
    let page_size = ctx.config.max_documents.min(WINDOW);
    result.failures.push(failure(source,"coverage","Jobs expose listing metadata only. Full descriptions, last-seen dates, and publisher status are unavailable."));
    // `max_pages` bounds the reads across all queries. A query moves on to its next page only
    // while the provider says more rows exist.
    let mut reads = 0;
    let mut query_index = 0;
    let mut page = 1;
    while reads < ctx.config.max_pages
        && query_index < queries.len()
        && result.documents.len() < ctx.config.max_documents
    {
        reads += 1;
        let query = queries[query_index].clone();
        let reply = match read(
            ctx,
            key,
            "list_documents",
            jobs_args(&query, page, page_size),
        )
        .await
        {
            Ok(reply) => reply,
            Err(error) => {
                result
                    .failures
                    .push(failure(source, "search", error.message));
                break;
            }
        };
        let Some(rows) = reply.data["items"].as_array() else {
            result.failures.push(failure(
                source,
                "search_shape",
                format!(
                    "The jobs response lacks items; response: {}",
                    reply.artifact
                ),
            ));
            break;
        };
        let meta = json!({"original_question":question,"query":query,"query_strategy":"explicit_quoted_phrase_else_names_then_content_words_one_per_request","pagination":reply.data["pagination"],"hint":reply.data["hint"]});
        let before = result.documents.len();
        let remaining = ctx.config.max_documents.saturating_sub(before);
        append_rows(source, "jobs", rows, &reply, &meta, remaining, &mut result);
        for doc in &mut result.documents[before..] {
            doc.provenance["pagination"] = reply.data["pagination"].clone();
            doc.provenance["detail_status"] = json!("unavailable_for_external_jobs");
            doc.provenance["publication_date_status"] =
                json!("not_returned; created_at_is_ingestion_time");
            doc.provenance["status_verification"] =
                json!("provider_active_status_only; publisher_unverified");
            if doc.url.is_empty() {
                result.failures.push(failure(
                    source,
                    "provenance",
                    format!("{} has no source URL.", doc.id),
                ));
            }
        }
        if rows.len() > remaining {
            result.failures.push(failure(
                source,
                "truncation",
                format!(
                    "The document cap excluded job rows; complete page remains in {}.",
                    reply.artifact
                ),
            ));
        }
        let more = reply.data["pagination"]["hasMore"].as_bool();
        if more.is_none() {
            result.failures.push(failure(
                source,
                "pagination",
                format!(
                    "Jobs pagination lacks hasMore; response: {}",
                    reply.artifact
                ),
            ));
        }
        if more == Some(true) && !rows.is_empty() {
            if reads == ctx.config.max_pages || result.documents.len() >= ctx.config.max_documents {
                result.failures.push(failure(source,"truncation",format!("Job retrieval for {query:?} stopped at page {page} with hasMore=true; page or document bound.")));
                query_index += 1;
                break;
            }
            page += 1;
        } else {
            query_index += 1;
            page = 1;
        }
    }
    if query_index < queries.len() {
        result.failures.push(failure(
            source,
            "query_limit",
            format!(
                "The page or document limit left job queries unsent: {:?}",
                &queries[query_index..]
            ),
        ));
    }
    result
}

pub async fn fetch(ctx: &FetchContext, source: &Source, question: &str) -> Result<FetchResult> {
    let Some(kind) = source
        .id
        .strip_prefix("lumenloop.")
        .filter(|kind| COLLECTIONS.iter().any(|entry| entry.0 == *kind))
    else {
        bail!("Unknown LumenLoop source: {}", source.id);
    };
    let mut result = FetchResult::default();
    if ctx.config.max_pages == 0 || ctx.config.max_documents == 0 {
        result.failures.push(failure(
            source,
            "limit",
            "The page or document limit prevents discovery.",
        ));
        return Ok(result);
    }
    if ctx.config.fixture {
        return Ok(fixture(source, kind));
    }
    let key = match std::env::var("LUMENLOOP_API_KEY") {
        Ok(key) if !key.trim().is_empty() => key,
        _ => {
            result
                .failures
                .push(failure(source, "auth", "LUMENLOOP_API_KEY is missing."));
            return Ok(result);
        }
    };
    if kind == "jobs" {
        return Ok(fetch_jobs(ctx, source, question, &key).await);
    }
    let limit = ctx.config.max_documents.min(WINDOW);
    let (tool, args, rows_key) = match kind {
        "directory" => (
            "search_directory",
            json!({"query":question,"limit":limit}),
            "projects",
        ),
        "av_passages" => (
            "find_av_passages",
            json!({"query":question,"limit":limit}),
            "",
        ),
        "scf" => (
            "find_similar_scf_submissions",
            json!({"query":question,"limit":limit}),
            "",
        ),
        _ => (
            "search_content_semantic",
            json!({"query":question,"types":[kind],"limit":limit,"response_format":"detailed"}),
            kind,
        ),
    };
    let reply = match read(ctx, &key, tool, args).await {
        Ok(reply) => reply,
        Err(error) => {
            result
                .failures
                .push(failure(source, "search", error.message));
            return Ok(result);
        }
    };
    let meta: Value = reply
        .data
        .as_object()
        .map(|fields| {
            Value::Object(
                fields
                    .iter()
                    .filter(|(name, _)| {
                        name.starts_with('_')
                            || matches!(name.as_str(), "hint" | "note" | "match_mode")
                    })
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect(),
            )
        })
        .unwrap_or_else(|| json!({}));
    for name in ["__truncated", "_weak_match", "_note", "hint", "note"] {
        if let Some(value) = meta.get(name) {
            if value != &Value::Bool(false) && !value.is_null() {
                result.failures.push(failure(
                    source,
                    "search_limit",
                    format!(
                        "Upstream search annotation {name}={value}; response: {}",
                        reply.artifact
                    ),
                ));
            }
        }
    }
    let rows = if rows_key.is_empty() {
        reply.data.as_array()
    } else {
        reply.data[rows_key].as_array()
    };
    let Some(rows) = rows else {
        result.failures.push(failure(
            source,
            "search_shape",
            format!(
                "The response lacks the expected record array; response: {}",
                reply.artifact
            ),
        ));
        return Ok(result);
    };
    if rows.len() >= limit {
        result.failures.push(failure(source, "truncation", format!("The search reached its {limit}-record window. More results may exist; no continuation API is available.")));
    }
    append_rows(source, kind, rows, &reply, &meta, limit, &mut result);
    let mut halted = false;
    let mut scf_cache: HashMap<String, std::result::Result<Reply, ReadError>> = HashMap::new();
    for document in &mut result.documents {
        if !needs_detail(kind) {
            document.provenance["detail_status"] = json!("not_requested");
            document.provenance["detail_skip_reason"] =
                json!("detailed discovery record is already complete for this collection");
            if document.url.is_empty() {
                document.provenance["source_url_status"] = json!("missing");
                result.failures.push(failure(
                    source,
                    "provenance",
                    format!("{} has no source URL.", document.id),
                ));
            }
            continue;
        }
        if halted {
            document.provenance["detail_status"] = json!("skipped_after_access_or_rate_error");
            continue;
        }
        let row = document.provenance["discovery"].clone();
        let detail = if kind == "scf" {
            let project = row["linked_project_slug"]
                .as_str()
                .or_else(|| row["linked_project_slugs"].as_array()?.first()?.as_str());
            if let Some(project) = project {
                document.provenance["detail_lookup_project"] = json!(project);
                if let Some(cached) = scf_cache.get(project) {
                    cached.clone()
                } else {
                    let mut value =
                        read(ctx, &key, "get_scf_submissions", json!({"slug":project})).await;
                    // This lookup does not join the provider's multi-project mapping.
                    // Resolve the observed project slug to its real name, then retry once.
                    if let Err(first_error) = &value {
                        if !first_error.stop {
                            document.provenance["scf_slug_lookup_failure"] =
                                json!(first_error.message);
                            if let Some(path) = &first_error.artifact {
                                document.raw_artifacts.push(path.clone());
                            }
                            match read(
                                ctx,
                                &key,
                                "get_project",
                                json!({"slug":project,"compact":true}),
                            )
                            .await
                            {
                                Ok(project_reply) => {
                                    document.raw_artifacts.push(project_reply.artifact);
                                    if let Some(name) = project_reply.data["title"]
                                        .as_str()
                                        .filter(|name| !name.is_empty())
                                    {
                                        document.provenance["detail_lookup_name"] = json!(name);
                                        value = read(
                                            ctx,
                                            &key,
                                            "get_scf_submissions",
                                            json!({"name":name}),
                                        )
                                        .await;
                                    }
                                }
                                Err(error) => value = Err(error),
                            }
                        }
                    }
                    scf_cache.insert(project.into(), value.clone());
                    value
                }
            } else {
                Err(ReadError {
                    message: "No project mapping permits an SCF detail lookup.".into(),
                    stop: false,
                    artifact: None,
                })
            }
        } else if kind == "directory" {
            read(
                ctx,
                &key,
                "get_project",
                json!({"slug":row["slug"],"compact":false}),
            )
            .await
        } else if let Some(id) = numeric_id(&row) {
            read(
                ctx,
                &key,
                "get_document",
                json!({"collection":if kind == "av_passages" { "av" } else { kind },"id":id}),
            )
            .await
        } else {
            Err(ReadError {
                message: "The document ID cannot convert to an exact API number.".into(),
                stop: false,
                artifact: None,
            })
        };
        match detail {
            Ok(detail) => {
                if !document.raw_artifacts.contains(&detail.artifact) {
                    document.raw_artifacts.push(detail.artifact.clone());
                }
                let detail_row = if kind == "scf" {
                    detail.data["submissions"]
                        .as_array()
                        .and_then(|items| items.iter().find(|item| item["slug"] == row["slug"]))
                } else {
                    Some(&detail.data)
                };
                if let Some(detail_row) = detail_row.filter(|value| {
                    value.is_object() && identifier(value, kind) == identifier(&row, kind)
                }) {
                    apply_detail(document, detail_row, kind, &detail.artifact);
                } else {
                    document.provenance["detail_status"] = json!("missing_or_mismatched");
                    result.failures.push(failure(
                        source,
                        "detail",
                        format!(
                            "Detail missing or mismatched for {}; response: {}",
                            document.id, detail.artifact
                        ),
                    ));
                }
            }
            Err(error) => {
                halted = error.stop;
                if let Some(path) = &error.artifact {
                    document.raw_artifacts.push(path.clone());
                }
                document.provenance["detail_status"] = json!("failed");
                document.provenance["detail_failure"] = json!(error.message);
                result.failures.push(failure(
                    source,
                    "detail",
                    format!("{}: {}", document.id, error.message),
                ));
            }
        }
        if document.url.is_empty() {
            document.provenance["source_url_status"] = json!("missing");
            result.failures.push(failure(
                source,
                "provenance",
                format!("{} has no source URL.", document.id),
            ));
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn detail_reads_only_where_discovery_is_partial() {
        for kind in ["research", "directory", "scf"] {
            assert!(needs_detail(kind), "{kind} must keep its detail read");
        }
        for kind in ["articles", "av", "av_passages", "events", "proposals"] {
            assert!(!needs_detail(kind), "{kind} must skip its detail read");
        }
    }
    #[test]
    fn duplicate_ids_report_count_and_retain_raw() {
        let source = sources().remove(0);
        let reply = Reply {
            data: Value::Null,
            artifact: "raw/duplicates.body".into(),
        };
        let rows = vec![
            json!({"id":"1","title":"First"}),
            json!({"id":1,"title":"Repeated"}),
            json!({"id":"1","title":"Repeated again"}),
        ];
        let mut result = FetchResult::default();
        append_rows(
            &source,
            "articles",
            &rows,
            &reply,
            &json!({}),
            20,
            &mut result,
        );
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].title, "First");
        assert_eq!(
            result.documents[0].raw_artifacts,
            vec!["raw/duplicates.body"]
        );
        let warning = result
            .failures
            .iter()
            .find(|f| f.stage == "lumenloop.duplicates")
            .expect("Duplicate omission must be reported");
        assert!(warning.message.contains("2 duplicate"));
        assert!(warning.message.contains("raw/duplicates.body"));
    }
    #[test]
    fn jobs_queries_try_names_first_then_content_words_without_a_term_list() {
        let queries = jobs_queries("Which Kotlin roles are open at Acme Pay for mobile work?");
        assert_eq!(queries[..2], ["Kotlin".to_owned(), "Acme Pay".to_owned()]);
        assert!(queries.iter().any(|q| q == "mobile"));
        assert_eq!(
            jobs_queries("List \"platform lead\" jobs"),
            ["platform lead"]
        );
        assert_eq!(
            jobs_queries("Find jobs mentioning \"alpha\" or \"beta\""),
            ["alpha", "beta"]
        );
        // An unclosed quote and a long quoted span are not phrases.
        assert_eq!(jobs_queries("Find \"Kotlin roles"), ["Kotlin", "roles"]);
        assert_eq!(
            jobs_queries("Find Kotlin roles. Data: \"one two three four five six\""),
            ["Kotlin", "roles"]
        );
        assert_eq!(jobs_queries("List jobs"), [""]);
        assert_eq!(
            jobs_args("Kotlin", 2, 5),
            json!({"collection":"jobs","search":"Kotlin","status":"active","sort":"published_at","order":"DESC","page":2,"limit":5})
        );
        assert!(sources().iter().any(|source| source.id == "lumenloop.jobs"));
    }
    #[test]
    fn job_metadata_keeps_status_dates_and_body_gap() {
        let row = json!({"id":"70","title":"Rust engineer","status":"active","created_at":"2026-09-18T20:08:08Z"});
        let (text, kind, full) = record_text(&row, "jobs");
        assert_eq!(kind, "job_catalog_metadata");
        assert!(!full);
        assert!(text.contains("active"));
        assert!(text.contains("2026-09-18"));
    }
    #[test]
    fn duplicate_rows_from_later_pages_keep_both_artifacts() {
        let source = sources().remove(0);
        let mut result = FetchResult::default();
        for artifact in ["raw/page1.body", "raw/page2.body"] {
            let reply = Reply {
                data: Value::Null,
                artifact: artifact.into(),
            };
            append_rows(
                &source,
                "articles",
                &[json!({"id":1})],
                &reply,
                &json!({}),
                2,
                &mut result,
            );
        }
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].raw_artifacts.len(), 2);
        assert_eq!(
            result.documents[0].provenance["duplicate_records"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert!(result.failures[0].message.contains("1 duplicate"));
    }
    #[test]
    fn decimal_string_ids_convert_without_rounding() {
        assert_eq!(numeric_id(&json!({"id":"10665"})), Some(10665));
        assert_eq!(numeric_id(&json!({"id":169})), Some(169));
        assert_eq!(numeric_id(&json!({"id":"9007199254740992"})), None);
        assert_eq!(numeric_id(&json!({"id":"x"})), None);
    }
    #[test]
    fn success_text_is_not_a_document() {
        assert!(decode(
            200,
            json!({"success":true,"data":{"text":"No document found"},"meta":{"format":"text"}}),
            "raw/test.body"
        )
        .is_err());
        assert!(
            decode(429, json!({"success":false}), "raw/test.body")
                .unwrap_err()
                .stop
        );
        assert!(
            decode(401, json!({"success":false}), "raw/test.body")
                .unwrap_err()
                .stop
        );
    }
    #[test]
    fn detail_keeps_discovery_and_summary_classification() {
        let source = sources().remove(0);
        let row = json!({"id":"10665","title":"Title","url":"https://example.invalid/article","summary":"Short", "similarity":0.3});
        let reply = Reply {
            data: Value::Null,
            artifact: "raw/search.body".into(),
        };
        let mut doc = make_document(
            &source,
            "articles",
            &row,
            &reply,
            &json!({"_weak_match":true}),
        )
        .unwrap();
        apply_detail(
            &mut doc,
            &json!({"id":10665,"long_summary":"Complete available summary"}),
            "articles",
            "raw/detail.body",
        );
        assert_eq!(doc.provenance["content_kind"], "ai_summary");
        assert_eq!(doc.provenance["full_source_body_available"], false);
        assert_eq!(doc.provenance["discovery"]["similarity"], 0.3);
        assert!(doc.text.contains("Complete available summary"));
        assert_eq!(doc.raw_artifacts.len(), 2);
    }
    #[test]
    fn av_passages_keep_offsets_and_use_parent_ids() {
        let row = json!({"av_id":"1310", "start_offset":1500, "summary":"Provider summary"});
        assert_eq!(identifier(&row, "av_passages"), Some("1310".into()));
        assert_eq!(numeric_id(&row), Some(1310));
        assert_eq!(record_text(&row, "av_passages").1, "ai_summary");
        assert_eq!(record_url(&row, "av_passages"), "");
        assert_eq!(
            identifier(&json!({"id":1310}), "av_passages"),
            Some("1310".into())
        );
    }
    #[test]
    fn failed_detail_keeps_an_artifact_reference() {
        let error = decode(
            200,
            json!({"success":true,"data":{"text":"No document found"}}),
            "raw/missing.body",
        )
        .unwrap_err();
        assert_eq!(error.artifact.as_deref(), Some("raw/missing.body"));
        assert!(!error.stop);
    }
    #[test]
    fn editorial_and_application_bodies_remain_exact() {
        let body = "# Header\n\nComplete\nbody.";
        assert_eq!(
            record_text(&json!({"body":body}), "research"),
            (body.into(), "stored_editorial_body", true)
        );
        assert_eq!(
            record_text(&json!({"application":body}), "scf"),
            (body.into(), "stored_application_body", true)
        );
        assert_eq!(
            record_url(&json!({"slug":"has-no-url","url":null}), "research"),
            ""
        );
    }
    #[tokio::test]
    async fn fixture_is_offline_and_limits_are_enforced() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = RunConfig {
            fixture: true,
            ..Default::default()
        };
        let http = crate::http::HttpRecorder::new(dir.path(), &config).unwrap();
        let ctx = FetchContext {
            http: http.clone(),
            config: config.clone(),
        };
        for source in sources() {
            let result = fetch(&ctx, &source, "Stellar").await.unwrap();
            assert_eq!(result.documents.len(), 1);
            assert_eq!(result.documents[0].provenance["fixture"], true);
            assert!(result.documents[0].raw_artifacts.is_empty());
        }
        assert_eq!(
            std::fs::read_dir(dir.path().join("raw")).unwrap().count(),
            0
        );
        config.max_documents = 0;
        let result = fetch(&FetchContext { http, config }, &sources()[0], "Stellar")
            .await
            .unwrap();
        assert!(result.documents.is_empty());
        assert_eq!(result.failures.len(), 1);
    }
}
