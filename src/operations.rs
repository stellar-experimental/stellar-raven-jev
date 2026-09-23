//! Typed retrieval operations. Query text is data. Endpoints stay fixed.
mod vocabulary;
use crate::types::{Document, Failure, FetchResult};
use anyhow::{bail, Context, Result};
use reqwest::Method;
use serde_json::{json, Map, Value};

const LUMENLOOP_SEMANTIC_URL: &str = "https://api.lumenloop.com/v1/tools/search_content_semantic";
const SCOUT_PROJECTS_URL: &str = "https://stellarlight.xyz/api/projects/search";

const SEMANTIC_TYPES: &[&str] = &[
    "articles",
    "av",
    "events",
    "research",
    "proposals",
    "tweets",
    "twitter_accounts",
    "scf_submissions",
    "knowledge",
];
const DATE_FIELDS: &[&str] = &["publishing_date", "created_at"];
const PROJECT_STATUSES: &[&str] = &["Development", "Pre-Release", "Live", "Inactive"];
const TEXT_FIELDS: &[&str] = &[
    "body",
    "application",
    "content",
    "chunk_text",
    "long_summary",
    "summary",
    "description",
    "body_preview",
    "title",
];

pub fn catalog() -> serde_json::Value {
    json!({
        "schema_version": 1,
        "operations": [
            {
                "operation": "connector.search",
                "arguments": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["source_id", "query"],
                    "properties": {
                        "source_id": {"type": "string", "enum": crate::connectors::sources().iter().map(|source| &source.id).collect::<Vec<_>>()},
                        "query": {"type": "string", "minLength": 1}
                    }
                },
                "search_semantics": "Sends query to the existing connector for source_id. The connector can rewrite query. This operation takes no endpoint and no extra filters.",
                "provider_limit": null,
                "text_scope": "Unchanged text from the existing connector.",
                "sources": crate::connectors::sources(),
                "continuation_supported": "source-dependent",
                "continuation_note": "max_pages bounds adapter attempts. It does not imply a provider cursor or exhaustive pagination."
            },
            {
                "operation": "lumenloop.semantic",
                "arguments": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["query", "types", "limit"],
                    "properties": {
                        "query": {"type": "string", "minLength": 1},
                        "types": {"type": "array", "minItems": 1, "items": {"type": "string", "enum": SEMANTIC_TYPES}},
                        "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                        "date_start": {"type": "string"},
                        "date_end": {"type": "string"},
                        "date_field": {"type": "string", "enum": DATE_FIELDS, "description":"Applies to article dates only."},
                        "sources": {"type": "array", "minItems": 1, "items": {"type": "string", "minLength": 1}}
                    }
                },
                "search_semantics": "Posts query, types, limit, dates, date_field, and sources to search_content_semantic. Values are not rewritten. The request sets response_format to detailed so long_summary is kept. Limit is the maximum rows for each content type. When a call document allowance is smaller, admission alternates requested types in their supplied order.",
                "provider_limit": 100,
                "text_scope": "Every non-empty string on the search row. long_summary comes before summary. This call does not fetch get_document.",
                "continuation_supported": false
            },
            {
                "operation": "scout.projects",
                "arguments": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["limit"],
                    "properties": {
                        "query": {"type": "string", "minLength": 1},
                        "status": {"type": "string", "enum": PROJECT_STATUSES},
                        "scf_awarded": {"type": "boolean"},
                        "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                        "offset": {"type": "integer", "minimum": 0}
                    }
                },
                "search_semantics": "Gets /api/projects/search. query is sent as q. scf_awarded is sent as scfAwarded. status, limit, and offset are sent as given. False scfAwarded clears the funded-only restriction. It does not select unfunded projects. The request omits fields.",
                "provider_limit": 100,
                "text_scope": "The complete project row. codeReferences are separate documents when the response includes them.",
                "continuation_supported": true
            },
            vocabulary::catalog_entry()
        ]
    })
}

pub fn validate(operation: &str, arguments: &serde_json::Value) -> Result<()> {
    let map = arguments
        .as_object()
        .context("arguments must be an object")?;
    match operation {
        "connector.search" => validate_connector(map),
        "lumenloop.semantic" => validate_semantic(map),
        "scout.projects" => validate_projects(map),
        "lumenloop.vocabulary" => vocabulary::validate(map),
        _ => bail!("unknown operation: {operation}"),
    }
}

pub async fn fetch(
    ctx: &crate::types::FetchContext,
    operation: &str,
    arguments: &serde_json::Value,
) -> Result<FetchResult> {
    validate(operation, arguments)?;
    if ctx.config.max_pages == 0 || ctx.config.max_documents == 0 {
        return Ok(stopped(
            operation,
            "The page or document limit prevents retrieval.",
        ));
    }
    match operation {
        "connector.search" => fetch_connector(ctx, arguments).await,
        "lumenloop.semantic" => fetch_semantic(ctx, arguments, lumenloop_key().as_deref()).await,
        "scout.projects" => fetch_projects(ctx, arguments).await,
        "lumenloop.vocabulary" => vocabulary::fetch(ctx, arguments).await,
        _ => bail!("unknown operation: {operation}"),
    }
}

fn validate_connector(map: &Map<String, Value>) -> Result<()> {
    unknown(map, &["source_id", "query"])?;
    let source_id = required_text(map, "source_id")?;
    required_text(map, "query")?;
    if !crate::connectors::sources()
        .iter()
        .any(|source| source.id == source_id)
    {
        bail!("unknown source_id");
    }
    Ok(())
}

fn validate_semantic(map: &Map<String, Value>) -> Result<()> {
    unknown(
        map,
        &[
            "query",
            "types",
            "limit",
            "date_start",
            "date_end",
            "date_field",
            "sources",
        ],
    )?;
    required_text(map, "query")?;
    required_types(map)?;
    required_limit(map, 100)?;
    let start = optional_bound(map, "date_start")?;
    let end = optional_bound(map, "date_end")?;
    if let (Some(start), Some(end)) = (start, end) {
        if !ordered(&start, &end) {
            bail!("date_start must not be after date_end");
        }
    }
    optional_enum(map, "date_field", DATE_FIELDS)?;
    optional_sources(map)?;
    Ok(())
}

fn validate_projects(map: &Map<String, Value>) -> Result<()> {
    unknown(map, &["query", "status", "scf_awarded", "limit", "offset"])?;
    optional_text(map, "query")?;
    optional_enum(map, "status", PROJECT_STATUSES)?;
    optional_bool(map, "scf_awarded")?;
    required_limit(map, 100)?;
    optional_offset(map)?;
    Ok(())
}

fn unknown(map: &Map<String, Value>, allowed: &[&str]) -> Result<()> {
    let mut names: Vec<_> = map
        .keys()
        .filter(|key| !allowed.contains(&key.as_str()))
        .cloned()
        .collect();
    if names.is_empty() {
        return Ok(());
    }
    names.sort();
    bail!("unknown arguments: {}", names.join(", "));
}

fn required_text(map: &Map<String, Value>, key: &str) -> Result<String> {
    match map.get(key) {
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(value.clone()),
        Some(Value::String(_)) => bail!("{key} must not be empty"),
        Some(_) => bail!("{key} must be a string"),
        None => bail!("{key} is required"),
    }
}

fn optional_text(map: &Map<String, Value>, key: &str) -> Result<Option<String>> {
    match map.get(key) {
        None => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value.clone())),
        Some(Value::String(_)) => bail!("{key} must not be empty"),
        Some(_) => bail!("{key} must be a string"),
    }
}

fn required_limit(map: &Map<String, Value>, max: u64) -> Result<u64> {
    let value = map.get("limit").context("limit is required")?;
    let number = value.as_u64().context("limit must be an integer")?;
    if !(1..=max).contains(&number) {
        bail!("limit must be from 1 through {max}");
    }
    Ok(number)
}

fn optional_offset(map: &Map<String, Value>) -> Result<Option<u64>> {
    match map.get("offset") {
        None => Ok(None),
        Some(value) => Ok(Some(
            value
                .as_u64()
                .context("offset must be a non-negative integer")?,
        )),
    }
}

fn optional_bool(map: &Map<String, Value>, key: &str) -> Result<Option<bool>> {
    match map.get(key) {
        None => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => bail!("{key} must be a boolean"),
    }
}

fn optional_enum(map: &Map<String, Value>, key: &str, allowed: &[&str]) -> Result<Option<String>> {
    match optional_text(map, key)? {
        None => Ok(None),
        Some(value) if allowed.contains(&value.as_str()) => Ok(Some(value)),
        Some(_) => bail!("{key} is not an accepted value"),
    }
}

fn required_types(map: &Map<String, Value>) -> Result<()> {
    let items = map
        .get("types")
        .context("types is required")?
        .as_array()
        .context("types must be an array")?;
    if items.is_empty() {
        bail!("types must not be empty");
    }
    for item in items {
        let kind = item.as_str().context("types entries must be strings")?;
        if !SEMANTIC_TYPES.contains(&kind) {
            bail!("types entry is not an accepted value");
        }
    }
    Ok(())
}

fn optional_sources(map: &Map<String, Value>) -> Result<()> {
    let Some(value) = map.get("sources") else {
        return Ok(());
    };
    let items = value.as_array().context("sources must be an array")?;
    if items.is_empty() {
        bail!("sources must not be empty");
    }
    for item in items {
        let source = item.as_str().context("sources entries must be strings")?;
        if source.trim().is_empty() {
            bail!("sources entries must not be empty");
        }
    }
    Ok(())
}

fn optional_bound(map: &Map<String, Value>, key: &str) -> Result<Option<Bound>> {
    match map.get(key) {
        None => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => {
            Ok(Some(parse_bound(value).with_context(|| {
                format!("{key} must be an ISO date or timestamp")
            })?))
        }
        Some(Value::String(_)) => bail!("{key} must not be empty"),
        Some(_) => bail!("{key} must be a string"),
    }
}

struct Bound {
    days: i64,
    instant: Option<(i64, i64)>,
}

fn ordered(start: &Bound, end: &Bound) -> bool {
    start.instant.unwrap_or((start.days * 86400, 0)) <= end.instant.unwrap_or((end.days * 86400, 0))
}

fn parse_bound(input: &str) -> Result<Bound> {
    let (input, year) = take_u32(input, 4)?;
    if year == 0 {
        bail!("invalid date");
    }
    let input = expect(input, "-")?;
    let (input, month) = take_u32(input, 2)?;
    let input = expect(input, "-")?;
    let (input, day) = take_u32(input, 2)?;
    if !(1..=12).contains(&month) || !valid_day(year, month, day) {
        bail!("invalid date");
    }
    let days = days_from_civil(year as i32, month, day);
    if input.is_empty() {
        return Ok(Bound {
            days,
            instant: None,
        });
    }
    let input = expect(input, "T")?;
    let (input, hour) = take_u32(input, 2)?;
    let input = expect(input, ":")?;
    let (input, minute) = take_u32(input, 2)?;
    let input = expect(input, ":")?;
    let (input, second) = take_u32(input, 2)?;
    if hour > 23 || minute > 59 || second > 59 {
        bail!("invalid time");
    }
    let (input, fraction) = parse_fraction(input)?;
    let (input, offset) = parse_offset(input)?;
    if !input.is_empty() {
        bail!("invalid timestamp");
    }
    let absolute = days * 86400 + hour as i64 * 3600 + minute as i64 * 60 + second as i64 - offset;
    Ok(Bound {
        days,
        instant: Some((absolute, fraction)),
    })
}

fn take_u32(input: &str, width: usize) -> Result<(&str, u32)> {
    let head = input.get(..width).context("invalid date")?;
    if !head.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("invalid date");
    }
    Ok((&input[width..], head.parse::<u32>()?))
}

fn expect<'a>(input: &'a str, token: &str) -> Result<&'a str> {
    input.strip_prefix(token).context("invalid date")
}

fn parse_fraction(input: &str) -> Result<(&str, i64)> {
    let Some(rest) = input.strip_prefix('.') else {
        return Ok((input, 0));
    };
    let digits = rest
        .bytes()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if !(1..=9).contains(&digits) {
        bail!("invalid timestamp");
    }
    let mut padded = rest[..digits].to_owned();
    while padded.len() < 9 {
        padded.push('0');
    }
    Ok((&rest[digits..], padded.parse::<i64>()?))
}

fn parse_offset(input: &str) -> Result<(&str, i64)> {
    if let Some(rest) = input.strip_prefix('Z') {
        return Ok((rest, 0));
    }
    let (sign, rest) = if let Some(rest) = input.strip_prefix('+') {
        (1i64, rest)
    } else if let Some(rest) = input.strip_prefix('-') {
        (-1i64, rest)
    } else {
        bail!("invalid timestamp");
    };
    let (rest, hour) = take_u32(rest, 2)?;
    let rest = expect(rest, ":")?;
    let (rest, minute) = take_u32(rest, 2)?;
    if hour > 23 || minute > 59 {
        bail!("invalid timestamp");
    }
    Ok((rest, sign * (hour as i64 * 3600 + minute as i64 * 60)))
}

fn valid_day(year: u32, month: u32, day: u32) -> bool {
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let max = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => 0,
    };
    (1..=max).contains(&day)
}

fn days_from_civil(mut y: i32, m: u32, d: u32) -> i64 {
    y -= i32::from(m <= 2);
    let era = y / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era as i64 * 146097 + doe as i64 - 719468
}

fn lumenloop_key() -> Option<String> {
    std::env::var("LUMENLOOP_API_KEY")
        .ok()
        .filter(|key| !key.trim().is_empty())
}

async fn fetch_connector(
    ctx: &crate::types::FetchContext,
    arguments: &Value,
) -> Result<FetchResult> {
    let source_id = arguments["source_id"].as_str().context("source_id")?;
    let query = arguments["query"].as_str().context("query")?;
    let source = crate::connectors::sources()
        .into_iter()
        .find(|source| source.id == source_id)
        .context("unknown source_id")?;
    crate::connectors::fetch(ctx, &source, query).await
}

async fn fetch_semantic(
    ctx: &crate::types::FetchContext,
    arguments: &Value,
    key: Option<&str>,
) -> Result<FetchResult> {
    if ctx.config.max_pages == 0 || ctx.config.max_documents == 0 {
        return Ok(stopped(
            "lumenloop.semantic",
            "The page or document limit prevents retrieval.",
        ));
    }
    if ctx.config.fixture {
        return Ok(fixture_semantic(arguments));
    }
    let Some(key) = key else {
        return Ok(stopped(
            "lumenloop.semantic",
            "LUMENLOOP_API_KEY is missing.",
        ));
    };
    let body = semantic_body(arguments);
    let limit = arguments["limit"].as_u64().context("limit")?;
    let (value, artifact, status) = match request_json(
        ctx,
        Method::POST,
        LUMENLOOP_SEMANTIC_URL,
        vec![
            ("Authorization".into(), format!("Bearer {key}")),
            ("Accept".into(), "application/json".into()),
        ],
        Some(body),
    )
    .await
    {
        Ok(response) => response,
        Err(message) => {
            return Ok(stopped(
                "lumenloop.semantic",
                &format!("LumenLoop read failed: {message}"),
            ))
        }
    };
    let payloads = match lumenloop_payloads(status, value) {
        Ok(payloads) => payloads,
        Err(message) => {
            return Ok(stopped(
                "lumenloop.semantic",
                &format!("{message}. Artifact: {artifact}"),
            ))
        }
    };
    let mut result = FetchResult::default();
    collect_semantic(
        &mut result,
        arguments,
        &payloads,
        &artifact,
        limit,
        ctx.config.max_documents,
    );
    Ok(result)
}

fn semantic_body(arguments: &Value) -> Value {
    let mut body = Map::new();
    for key in ["query", "types", "limit"] {
        body.insert(key.into(), arguments[key].clone());
    }
    // The saved prose says the external default can omit long_summary.
    body.insert("response_format".into(), json!("detailed"));
    for key in ["date_start", "date_end", "date_field", "sources"] {
        if let Some(value) = arguments.get(key) {
            body.insert(key.into(), value.clone());
        }
    }
    Value::Object(body)
}

struct SemanticPayload {
    block_index: Option<usize>,
    data: Value,
}

fn lumenloop_payloads(
    status: u16,
    value: Value,
) -> std::result::Result<Vec<SemanticPayload>, String> {
    if !(200..300).contains(&status) || value.get("success") != Some(&Value::Bool(true)) {
        let code = value
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or("unspecified");
        let error = value.get("error").and_then(Value::as_str).unwrap_or("");
        let hint = value.get("hint").and_then(Value::as_str).unwrap_or("");
        return Err(format!(
            "HTTP {status}; API code {code}; error {error}; hint {hint}"
        ));
    }
    let data = value.get("data").cloned().unwrap_or(Value::Null);
    if value.pointer("/meta/format").and_then(Value::as_str) != Some("blocks") {
        return Ok(vec![SemanticPayload {
            block_index: None,
            data,
        }]);
    }
    let Some(blocks) = data.get("content").and_then(Value::as_array) else {
        return Err("Blocks response has no content array".into());
    };
    if blocks.is_empty() {
        return Err("Blocks response has no content blocks".into());
    }
    // Keep every block. A later block must not disappear behind the first parse.
    let mut payloads = Vec::with_capacity(blocks.len());
    for (index, block) in blocks.iter().enumerate() {
        let data = match block.get("text").and_then(Value::as_str) {
            Some(text) => {
                serde_json::from_str::<Value>(text).unwrap_or_else(|_| json!({"text": text}))
            }
            None => json!({"block": block}),
        };
        payloads.push(SemanticPayload {
            block_index: Some(index),
            data,
        });
    }
    Ok(payloads)
}

fn requested_types(arguments: &Value) -> Vec<&str> {
    arguments
        .get("types")
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default()
}

fn collect_semantic(
    result: &mut FetchResult,
    arguments: &Value,
    payloads: &[SemanticPayload],
    artifact: &str,
    limit: u64,
    max_documents: usize,
) {
    let requested = requested_types(arguments);
    let mut groups: Vec<std::collections::VecDeque<Document>> =
        requested.iter().map(|_| Default::default()).collect();
    for kind in &requested {
        if !payloads
            .iter()
            .any(|payload| payload.data.get(*kind).is_some_and(Value::is_array))
        {
            failure(result, "lumenloop.semantic", format!("The response omitted requested group {kind}; absence is not a verified empty result. Artifact: {artifact}"));
        }
    }
    for payload in payloads {
        semantic_annotations(&payload.data, artifact, result);
        let metadata = search_metadata(&payload.data);
        let mut matched = false;
        for (index, kind) in requested.iter().enumerate() {
            let Some(rows) = payload.data.get(*kind).and_then(Value::as_array) else {
                continue;
            };
            matched = true;
            if rows.len() as u64 >= limit {
                failure(result, "lumenloop.semantic", format!("{kind} reached its {limit}-row window. No continuation API exists. Artifact: {artifact}"));
            }
            for row in rows {
                match semantic_document(kind, row, artifact, &metadata, limit) {
                    Some(document) => groups[index].push_back(document),
                    None => failure(
                        result,
                        "lumenloop.semantic",
                        format!("A {kind} search record has no usable ID. Artifact: {artifact}"),
                    ),
                }
            }
        }
        if !matched {
            note_unparsed_payload(result, payload, artifact);
        }
    }
    // Preserve an exploration slot for each requested collection before adding depth.
    // This avoids consuming the entire allowance in JSON-key order.
    while groups.iter().any(|group| !group.is_empty()) && result.documents.len() < max_documents {
        for group in &mut groups {
            if result.documents.len() >= max_documents {
                break;
            }
            if let Some(document) = group.pop_front() {
                store_document(result, document);
            }
        }
    }
    let omitted: usize = groups.iter().map(|group| group.len()).sum();
    if omitted > 0 {
        failure(result, "lumenloop.semantic", format!("The document limit omitted {omitted} returned records after alternating requested collections. Complete provider rows remain in {artifact}."));
    }
    for document in groups.into_iter().flatten() {
        retain_semantic_omission(result, document);
    }
}

fn retain_semantic_omission(result: &mut FetchResult, mut document: Document) {
    if !document.provenance.is_object() {
        document.provenance = json!({"source_provenance":document.provenance});
    }
    let mut admission = json!({"reason":"call_document_limit"});
    if let Some(prior) = document.provenance.get("admission") {
        admission["prior_admission"] = prior.clone();
    }
    document.provenance["admission"] = admission;
    result.omitted_documents.push(document);
}

fn note_unparsed_payload(result: &mut FetchResult, payload: &SemanticPayload, artifact: &str) {
    let label = match payload.block_index {
        Some(index) => format!("Block {index}"),
        None => "The response".into(),
    };
    if let Some(text) = payload
        .data
        .as_str()
        .or_else(|| payload.data.get("text").and_then(Value::as_str))
    {
        failure(
            result,
            "lumenloop.semantic",
            format!("{label} has no requested records. Text: {text}. Artifact: {artifact}"),
        );
        return;
    }
    let body = serde_json::to_string(&payload.data).unwrap_or_default();
    failure(
        result,
        "lumenloop.semantic",
        format!("{label} has no requested record arrays. Body: {body}. Artifact: {artifact}"),
    );
}

fn semantic_annotations(data: &Value, artifact: &str, result: &mut FetchResult) {
    for name in [
        "__truncated",
        "_weak_match",
        "_note",
        "_hint",
        "hint",
        "note",
    ] {
        if let Some(value) = data.get(name) {
            if value != &Value::Bool(false) && !value.is_null() {
                failure(
                    result,
                    "lumenloop.semantic",
                    format!("Upstream annotation {name}={value}. Artifact: {artifact}"),
                );
            }
        }
    }
}

fn search_metadata(data: &Value) -> Value {
    let Some(map) = data.as_object() else {
        return json!({});
    };
    Value::Object(
        map.iter()
            .filter(|(key, value)| key.starts_with('_') || !value.is_array())
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    )
}

fn semantic_document(
    kind: &str,
    row: &Value,
    artifact: &str,
    metadata: &Value,
    limit: u64,
) -> Option<Document> {
    let id = row_id(row)?;
    let full_body = ["body", "application", "content"].iter().any(|key| {
        row.get(*key)
            .and_then(Value::as_str)
            .is_some_and(|text| !text.is_empty())
    });
    Some(Document {
        id: format!("lumenloop.{kind}:{id}"),
        source_id: format!("lumenloop.{kind}"),
        title: first_str(row, &["title", "name"]).unwrap_or(&id).to_owned(),
        url: semantic_url(row),
        text: available_text(row),
        provenance: json!({
            "provider": "lumenloop",
            "operation": "lumenloop.semantic",
            "tool": "search_content_semantic",
            "collection": kind,
            "record_id": id,
            "request_url": LUMENLOOP_SEMANTIC_URL,
            "discovery": row,
            "search_metadata": metadata,
            "content_scope": "search_row_available_text",
            "full_source_body_available": full_body,
            "pagination": {"mode": "top_k", "continuation_supported": false, "requested_limit": limit},
            "upstream_scores_are_calibrated": false
        }),
        raw_artifacts: vec![artifact.to_owned()],
    })
}

fn semantic_url(row: &Value) -> String {
    if let Some(url) = first_str(row, &["url", "submission_url"]) {
        return url.to_owned();
    }
    link_url(row.get("link")).unwrap_or_default()
}

fn link_url(value: Option<&Value>) -> Option<String> {
    let value = value?;
    if let Some(text) = value.as_str().filter(|text| !text.is_empty()) {
        return Some(text.to_owned());
    }
    first_str(value, &["url", "href"]).map(str::to_owned)
}

fn available_text(row: &Value) -> String {
    let Some(map) = row.as_object() else {
        return serde_json::to_string_pretty(row).unwrap_or_default();
    };
    let mut parts = Vec::new();
    for key in TEXT_FIELDS {
        push_text(&mut parts, map.get(*key));
    }
    for (key, value) in map {
        if !TEXT_FIELDS.contains(&key.as_str()) {
            push_text(&mut parts, Some(value));
        }
    }
    if parts.is_empty() {
        serde_json::to_string_pretty(row).unwrap_or_default()
    } else {
        parts.join("\n\n")
    }
}

fn push_text(parts: &mut Vec<String>, value: Option<&Value>) {
    let Some(text) = value
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
    else {
        return;
    };
    if !parts.iter().any(|item| item == text) {
        parts.push(text.to_owned());
    }
}

async fn fetch_projects(
    ctx: &crate::types::FetchContext,
    arguments: &Value,
) -> Result<FetchResult> {
    if ctx.config.max_pages == 0 || ctx.config.max_documents == 0 {
        return Ok(stopped(
            "scout.projects",
            "The page or document limit prevents retrieval.",
        ));
    }
    if ctx.config.fixture {
        return Ok(fixture_projects(arguments));
    }
    let query = arguments.get("query").and_then(Value::as_str);
    let status = arguments.get("status").and_then(Value::as_str);
    let awarded = arguments.get("scf_awarded").and_then(Value::as_bool);
    let limit = arguments["limit"].as_u64().context("limit")?;
    let mut offset = arguments.get("offset").and_then(Value::as_u64).unwrap_or(0);
    let mut result = FetchResult::default();
    for page in 0..ctx.config.max_pages {
        if result.documents.len() >= ctx.config.max_documents {
            failure(
                &mut result,
                "scout.projects",
                "The document limit omitted later Scout pages.",
            );
            break;
        }
        let url = projects_url(query, status, awarded, limit, offset)?;
        let (value, artifact, status_code) =
            match request_json(ctx, Method::GET, &url, vec![], None).await {
                Ok(response) => response,
                Err(message) => {
                    failure(
                        &mut result,
                        "scout.projects",
                        format!("Scout read failed: {message}"),
                    );
                    break;
                }
            };
        if !(200..300).contains(&status_code) {
            failure(
                &mut result,
                "scout.projects",
                format!("Scout returned HTTP {status_code}. Artifact: {artifact}"),
            );
            break;
        }
        project_notices(&value["meta"], &mut result);
        let sent = sent_filters(query, status, awarded, limit, offset);
        if let Some(filters) = value.pointer("/meta/filters").and_then(Value::as_object) {
            for (key, expected) in sent.as_object().expect("filters object") {
                if let Some(actual) = filters.get(key) {
                    if actual != expected {
                        failure(&mut result, "scout.projects", format!("Provider filter echo differs for {key}. Inspect raw response {artifact}."));
                    }
                }
            }
        }
        for row in value
            .get("projects")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if status.is_some_and(|s| row["status"].as_str() != Some(s))
                || (awarded == Some(true) && row["scfAwarded"] != true)
            {
                failure(&mut result, "scout.projects", format!("A returned project lacks or contradicts a requested status or SCF award filter. Artifact: {artifact}"));
            }
        }
        let Some(projects) = value.get("projects").and_then(Value::as_array) else {
            failure(
                &mut result,
                "scout.projects",
                format!("Scout response omitted projects. Artifact: {artifact}"),
            );
            break;
        };
        let returned = projects.len() as u64;
        let mut capped = false;
        for row in projects {
            if !push_document(
                &mut result,
                ctx.config.max_documents,
                project_document(
                    row,
                    &artifact,
                    &url,
                    &value["meta"],
                    query,
                    status,
                    awarded,
                    limit,
                    offset,
                ),
                "scout.projects",
                &artifact,
            ) {
                capped = true;
                break;
            }
        }
        if !capped {
            if let Some(references) = value.get("codeReferences").and_then(Value::as_array) {
                for row in references {
                    if !push_document(
                        &mut result,
                        ctx.config.max_documents,
                        code_document(row, &artifact, &url).map(|mut document| {
                            document.provenance["meta"] = value["meta"].clone();
                            document.provenance["sent_filters"] = sent.clone();
                            document
                        }),
                        "scout.projects",
                        &artifact,
                    ) {
                        capped = true;
                        break;
                    }
                }
            }
        }
        if capped {
            break;
        }
        let total = value.pointer("/meta/counts/total").and_then(Value::as_u64);
        // The provider slices the keyword set by offset + limit before adding semantic rows.
        let next = offset.saturating_add(limit);
        match page_stop(returned, limit, next, total) {
            PageStop::Done => break,
            PageStop::UnknownTotal => {
                failure(
                    &mut result,
                    "scout.projects",
                    "Provider counts do not establish the page boundary. Pagination is not confirmed.",
                );
                break;
            }
            PageStop::More => {
                if page + 1 == ctx.config.max_pages {
                    failure(
                        &mut result,
                        "scout.projects",
                        format!("Scout retrieval stopped at the page limit. Next offset: {next}."),
                    );
                    break;
                }
                offset = next;
            }
        }
    }
    Ok(result)
}

fn projects_url(
    query: Option<&str>,
    status: Option<&str>,
    awarded: Option<bool>,
    limit: u64,
    offset: u64,
) -> Result<String> {
    let mut url = reqwest::Url::parse(SCOUT_PROJECTS_URL)?;
    {
        let mut pairs = url.query_pairs_mut();
        if let Some(query) = query {
            pairs.append_pair("q", query);
        }
        if let Some(status) = status {
            pairs.append_pair("status", status);
        }
        if let Some(awarded) = awarded {
            pairs.append_pair("scfAwarded", if awarded { "true" } else { "false" });
        }
        pairs.append_pair("limit", &limit.to_string());
        pairs.append_pair("offset", &offset.to_string());
    }
    Ok(url.into())
}

fn sent_filters(
    query: Option<&str>,
    status: Option<&str>,
    awarded: Option<bool>,
    limit: u64,
    offset: u64,
) -> Value {
    let mut filters = Map::new();
    if let Some(query) = query {
        filters.insert("q".into(), json!(query));
    }
    if let Some(status) = status {
        filters.insert("status".into(), json!(status));
    }
    if let Some(awarded) = awarded {
        filters.insert("scfAwarded".into(), json!(awarded));
    }
    filters.insert("limit".into(), json!(limit));
    filters.insert("offset".into(), json!(offset));
    Value::Object(filters)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PageStop {
    Done,
    More,
    UnknownTotal,
}

fn page_stop(returned: u64, limit: u64, next_offset: u64, total: Option<u64>) -> PageStop {
    match total {
        Some(total) if total > 0 && next_offset < total => PageStop::More,
        Some(total) if total > 0 => PageStop::Done,
        Some(0) if returned >= limit => PageStop::UnknownTotal,
        Some(0) => PageStop::Done,
        None if returned == 0 => PageStop::Done,
        None if returned < limit => PageStop::UnknownTotal,
        None => PageStop::More,
        _ => PageStop::UnknownTotal,
    }
}

fn project_notices(meta: &Value, result: &mut FetchResult) {
    let mode = meta.get("matchMode").and_then(Value::as_str).unwrap_or("");
    if mode == "semantic"
        || mode == "majority"
        || mode.starts_with("loose-")
        || meta["semantic"] == true
        || meta
            .pointer("/counts/semantic")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            > 0
    {
        failure(
            result,
            "scout.projects",
            "The provider used broad matching or semantic fallback. Inspect matchMode and each row before relying on relevance.",
        );
    }
    if let Some(summary) = meta.pointer("/advisory/summary").and_then(Value::as_str) {
        failure(
            result,
            "scout.projects",
            format!("Scout advisory: {summary}"),
        );
    }
    if let Some(to) = meta.pointer("/didYouMean/to").and_then(Value::as_str) {
        let from = meta
            .pointer("/didYouMean/from")
            .and_then(Value::as_str)
            .unwrap_or("");
        failure(
            result,
            "scout.projects",
            format!("Scout corrected the query from {from} to {to}."),
        );
    }
    for key in ["warnings", "sourceAdvisory", "exactMiss", "degraded"] {
        if let Some(value) = meta.get(key).filter(|value| !value.is_null()) {
            failure(result, "scout.projects", format!("Scout {key}: {value}"));
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn project_document(
    row: &Value,
    artifact: &str,
    request_url: &str,
    meta: &Value,
    query: Option<&str>,
    status: Option<&str>,
    awarded: Option<bool>,
    limit: u64,
    offset: u64,
) -> Option<Document> {
    let id = row_id(row)?;
    let url = first_str(row, &["url"])
        .or_else(|| row.pointer("/links/website").and_then(Value::as_str))
        .filter(|url| !url.is_empty())
        .unwrap_or(request_url);
    Some(Document {
        id: format!("stellarlight.projects:{id}"),
        source_id: "stellarlight.projects".into(),
        title: first_str(row, &["name", "title", "slug"])
            .unwrap_or(&id)
            .to_owned(),
        url: url.to_owned(),
        text: serde_json::to_string_pretty(row).unwrap_or_default(),
        provenance: json!({
            "provider": "stellarlight",
            "operation": "scout.projects",
            "content_scope": "structured_record",
            "record_id": id,
            "request_url": request_url,
            "filters": sent_filters(query, status, awarded, limit, offset),
            "meta": meta,
            "row": row,
            "pagination": {"continuation_supported": true, "limit": limit, "offset": offset},
            "upstream_scores_are_calibrated": false
        }),
        raw_artifacts: vec![artifact.to_owned()],
    })
}

fn code_document(row: &Value, artifact: &str, request_url: &str) -> Option<Document> {
    let id = row_id(row)?;
    Some(Document {
        id: format!("stellarlight.projects:code:{id}"),
        source_id: "stellarlight.projects".into(),
        title: first_str(row, &["fullName", "name", "title"])
            .unwrap_or(&id)
            .to_owned(),
        url: first_str(row, &["url", "htmlUrl"])
            .filter(|url| !url.is_empty())
            .unwrap_or(request_url)
            .to_owned(),
        text: serde_json::to_string_pretty(row).unwrap_or_default(),
        provenance: json!({
            "provider": "stellarlight",
            "operation": "scout.projects",
            "content_scope": "code_reference",
            "record_id": id,
            "request_url": request_url,
            "row": row
        }),
        raw_artifacts: vec![artifact.to_owned()],
    })
}

fn push_document(
    result: &mut FetchResult,
    max_documents: usize,
    document: Option<Document>,
    operation: &str,
    artifact: &str,
) -> bool {
    if result.documents.len() >= max_documents {
        failure(
            result,
            operation,
            "The document limit omitted returned records.",
        );
        return false;
    }
    let Some(document) = document else {
        failure(
            result,
            operation,
            format!("A row has no usable ID. Artifact: {artifact}"),
        );
        return true;
    };
    store_document(result, document);
    true
}

fn store_document(result: &mut FetchResult, mut document: Document) {
    let id = assigned_id(result, &document);
    if id != document.id {
        document.provenance["provider_document_id"] = json!(document.id);
        document.id = id;
    }
    result.documents.push(document);
}

fn assigned_id(result: &FetchResult, document: &Document) -> String {
    if let Some(existing) = result
        .documents
        .iter()
        .find(|existing| exact_repeat(existing, document))
    {
        return existing.id.clone();
    }
    let mut variant_ids = Vec::new();
    for existing in &result.documents {
        if same_record(existing, document) && !variant_ids.contains(&existing.id) {
            variant_ids.push(existing.id.clone());
        }
    }
    if variant_ids.is_empty() {
        document.id.clone()
    } else {
        format!("{}#{}", document.id, variant_ids.len())
    }
}

fn same_record(existing: &Document, document: &Document) -> bool {
    existing.source_id == document.source_id
        && existing.provenance.get("content_scope") == document.provenance.get("content_scope")
        && existing.provenance.get("record_id").is_some()
        && existing.provenance.get("record_id") == document.provenance.get("record_id")
}

fn exact_repeat(existing: &Document, document: &Document) -> bool {
    same_record(existing, document)
        && existing.text == document.text
        && existing.title == document.title
        && existing.url == document.url
}

fn row_id(row: &Value) -> Option<String> {
    for key in ["id", "slug", "av_id", "fullName", "name", "url"] {
        match row.get(key) {
            Some(Value::String(value)) if !value.is_empty() => return Some(value.clone()),
            Some(Value::Number(value)) => return Some(value.to_string()),
            _ => {}
        }
    }
    None
}

fn first_str<'a>(row: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|key| {
        row.get(*key)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
    })
}

async fn request_json(
    ctx: &crate::types::FetchContext,
    method: Method,
    url: &str,
    headers: Vec<(String, String)>,
    body: Option<Value>,
) -> std::result::Result<(Value, String, u16), String> {
    let response = ctx
        .http
        .request(method, url, headers, body)
        .await
        .map_err(|error| error.to_string())?;
    let artifact = response.artifact.clone();
    let status = response.status;
    let value = response
        .json()
        .map_err(|_| format!("Response is not valid JSON. Artifact: {artifact}"))?;
    Ok((value, artifact, status))
}

fn fixture_semantic(arguments: &Value) -> FetchResult {
    let query = arguments["query"].as_str().unwrap_or("");
    FetchResult {
        omitted_documents: vec![],
        documents: vec![Document {
            id: "lumenloop.semantic:fixture".into(),
            source_id: "lumenloop.semantic".into(),
            title: "Fixture semantic result".into(),
            url: "https://example.invalid/lumenloop/semantic".into(),
            text: format!("Offline fixture semantic result.\n\n{query}"),
            provenance: json!({
                "fixture": true,
                "provider": "lumenloop",
                "operation": "lumenloop.semantic",
                "request_url": LUMENLOOP_SEMANTIC_URL,
                "request_body": semantic_body(arguments),
                "content_scope": "synthetic_fixture",
                "continuation_supported": false
            }),
            raw_artifacts: vec![],
        }],
        failures: vec![],
    }
}

fn fixture_projects(arguments: &Value) -> FetchResult {
    let query = arguments.get("query").and_then(Value::as_str);
    let status = arguments.get("status").and_then(Value::as_str);
    let awarded = arguments.get("scf_awarded").and_then(Value::as_bool);
    let limit = arguments["limit"].as_u64().unwrap_or(0);
    let offset = arguments.get("offset").and_then(Value::as_u64).unwrap_or(0);
    let filters = sent_filters(query, status, awarded, limit, offset);
    FetchResult {
        omitted_documents: vec![],
        documents: vec![Document {
            id: "stellarlight.projects:fixture-operation".into(),
            source_id: "stellarlight.projects".into(),
            title: "Fixture Scout project".into(),
            url: "https://example.invalid/scout/project".into(),
            text: format!(
                "Offline fixture Scout project.\n\n{}",
                serde_json::to_string(&filters).unwrap_or_default()
            ),
            provenance: json!({
                "fixture": true,
                "provider": "stellarlight",
                "operation": "scout.projects",
                "request_url": SCOUT_PROJECTS_URL,
                "filters": filters,
                "content_scope": "synthetic_fixture",
                "continuation_supported": true
            }),
            raw_artifacts: vec![],
        }],
        failures: vec![],
    }
}

fn stopped(operation: &str, message: &str) -> FetchResult {
    let mut result = FetchResult::default();
    failure(&mut result, operation, message);
    result
}

fn failure(result: &mut FetchResult, operation: &str, message: impl Into<String>) {
    result.failures.push(Failure {
        stage: "operations".into(),
        source_id: Some(operation.into()),
        message: message.into(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{FetchContext, RunConfig};
    use std::path::Path;

    fn arguments(value: Value) -> Value {
        value
    }

    fn context(dir: &Path, fixture: bool, max_documents: usize, max_pages: usize) -> FetchContext {
        let config = RunConfig {
            fixture,
            output_dir: dir.to_path_buf(),
            max_documents,
            max_pages,
            ..RunConfig::default()
        };
        FetchContext {
            http: crate::http::HttpRecorder::new(dir, &config).unwrap(),
            config,
        }
    }

    fn raw_count(dir: &Path) -> usize {
        std::fs::read_dir(dir.join("raw"))
            .map(|entries| entries.flatten().count())
            .unwrap_or(0)
    }

    #[test]
    fn small_semantic_allowance_keeps_each_requested_collection() {
        let arguments = json!({"query":"storage","types":["research","articles"],"limit":2});
        let payloads = vec![SemanticPayload {
            block_index: None,
            data: json!({
            "articles":[{"id":1,"summary":"a1"},{"id":2,"summary":"a2"}],
            "research":[{"id":3,"summary":"r1"},{"id":4,"summary":"r2"}]}),
        }];
        let mut result = FetchResult::default();
        collect_semantic(&mut result, &arguments, &payloads, "raw/test.body", 2, 2);
        assert_eq!(
            result
                .documents
                .iter()
                .map(|d| d.source_id.as_str())
                .collect::<Vec<_>>(),
            vec!["lumenloop.research", "lumenloop.articles"]
        );
        assert!(result
            .failures
            .iter()
            .any(|f| f.message.contains("omitted 2")));
    }

    #[test]
    fn semantic_omissions_preserve_parsed_rows_without_changing_admission() {
        let arguments = json!({"query":"storage","types":["research","articles","av"],"limit":10});
        let data = json!({
            "research":[{"id":1,"summary":"r1"},{"id":2,"summary":"r2"}],
            "articles":[{"id":3,"summary":"a1"},{"id":4,"summary":"a2"}],
            "av":[{"id":5,"summary":"v1"},{"id":6,"summary":"v2"}]
        });
        let metadata = search_metadata(&data);
        let parsed: Vec<_> = ["research", "articles", "av"]
            .iter()
            .flat_map(|kind| {
                data[kind].as_array().unwrap().iter().map(|row| {
                    semantic_document(kind, row, "raw/test.body", &metadata, 10).unwrap()
                })
            })
            .collect();
        let payloads = vec![SemanticPayload {
            block_index: None,
            data,
        }];
        let mut full = FetchResult::default();
        collect_semantic(&mut full, &arguments, &payloads, "raw/test.body", 10, 6);
        let expected: Vec<_> = [0, 2, 4, 1, 3, 5].iter().map(|i| &parsed[*i]).collect();
        assert_eq!(
            serde_json::to_value(&full.documents).unwrap(),
            json!(expected)
        );
        assert!(full.omitted_documents.is_empty());
        assert!(full.failures.is_empty());
        for cap in 0..6 {
            let mut result = FetchResult::default();
            collect_semantic(&mut result, &arguments, &payloads, "raw/test.body", 10, cap);
            assert_eq!(
                serde_json::to_value(&result.documents).unwrap(),
                json!(&expected[..cap])
            );
            assert_eq!(result.omitted_documents.len(), 6 - cap);
            for omitted in &result.omitted_documents {
                assert!(!result.documents.iter().any(|d| d.id == omitted.id));
                assert_eq!(
                    omitted.provenance["admission"]["reason"],
                    "call_document_limit"
                );
                let mut original = omitted.clone();
                original
                    .provenance
                    .as_object_mut()
                    .unwrap()
                    .remove("admission");
                let parsed = parsed.iter().find(|d| d.id == omitted.id).unwrap();
                assert_eq!(serde_json::to_value(original).unwrap(), json!(parsed));
            }
            assert_eq!(result.failures.len(), 1);
            assert_eq!(result.failures[0].message, format!("The document limit omitted {} returned records after alternating requested collections. Complete provider rows remain in raw/test.body.", 6 - cap));
        }
    }

    #[test]
    fn semantic_omission_keeps_existing_provenance_and_provider_identity() {
        let document = Document {
            id: "provider:same".into(),
            source_id: "lumenloop.articles".into(),
            title: "Original".into(),
            url: "https://example.invalid/one".into(),
            text: "Exact UTF-8: naïve\n".into(),
            provenance: json!({"admission":{"reason":"prior","other":7},"record_id":"same"}),
            raw_artifacts: vec!["raw/original.body".into()],
        };
        let mut result = FetchResult::default();
        result.documents.push(document.clone());
        retain_semantic_omission(&mut result, document.clone());
        let omitted = &result.omitted_documents[0];
        assert_eq!(omitted.id, document.id);
        assert_eq!(omitted.text, document.text);
        assert_eq!(omitted.raw_artifacts, document.raw_artifacts);
        assert_eq!(
            omitted.provenance["admission"]["prior_admission"],
            document.provenance["admission"]
        );
        assert_eq!(omitted.provenance["record_id"], "same");
        let mut scalar = document;
        scalar.provenance = json!("original scalar");
        retain_semantic_omission(&mut result, scalar);
        assert_eq!(
            result.omitted_documents[1].provenance["source_provenance"],
            "original scalar"
        );
    }

    #[test]
    fn archived_fetch_result_defaults_to_no_reported_omissions() {
        let result: FetchResult =
            serde_json::from_value(json!({"documents":[],"failures":[]})).unwrap();
        assert!(result.omitted_documents.is_empty());
    }

    #[test]
    fn mixed_date_bounds_compare_utc_instants() {
        assert!(validate(
            "lumenloop.semantic",
            &json!({"query":"fees","types":["articles"],"limit":4,
            "date_start":"2026-09-21","date_end":"2026-09-21T00:00:00+14:00"})
        )
        .is_err());
        assert!(validate(
            "lumenloop.semantic",
            &json!({"query":"fees","types":["articles"],"limit":4,
            "date_start":"2026-09-21T00:00:00+14:00","date_end":"2026-09-21"})
        )
        .is_ok());
    }

    #[test]
    fn short_pages_do_not_hide_known_later_rows() {
        assert_eq!(page_stop(4, 20, 20, Some(40)), PageStop::More);
        assert_eq!(page_stop(4, 20, 20, None), PageStop::UnknownTotal);
    }

    #[test]
    fn loose_matching_and_semantic_topups_are_visible() {
        let mut result = FetchResult::default();
        project_notices(
            &json!({"matchMode":"loose-1","semantic":true,"counts":{"semantic":45}}),
            &mut result,
        );
        assert!(!result.failures.is_empty());
    }

    #[test]
    fn catalog_lists_the_four_operations() {
        let catalog = catalog();
        let operations = catalog["operations"].as_array().unwrap();
        assert_eq!(operations.len(), 4);
        assert_eq!(operations[0]["operation"], "connector.search");
        assert_eq!(operations[1]["operation"], "lumenloop.semantic");
        assert_eq!(operations[2]["operation"], "scout.projects");
        assert_eq!(operations[3]["operation"], "lumenloop.vocabulary");
        assert_eq!(operations[1]["provider_limit"], 100);
        assert_eq!(operations[1]["continuation_supported"], false);
        assert_eq!(operations[2]["provider_limit"], 100);
        assert_eq!(operations[2]["continuation_supported"], true);
        assert!(
            operations[1]["arguments"]["properties"]["types"]["items"]["enum"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == "articles")
        );
        assert!(operations[2]["arguments"]["properties"]["status"]["enum"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == "Inactive"));
        assert!(operations[2]["text_scope"]
            .as_str()
            .unwrap()
            .contains("complete project row"));
    }

    #[test]
    fn validate_rejects_unknown_arguments_and_bad_ranges() {
        let error = validate("nope", &json!({})).unwrap_err();
        assert!(error.to_string().contains("unknown operation"));
        let error = validate(
            "connector.search",
            &json!({"source_id": "stellarlight.projects", "query": "wallets", "url": "https://evil.example", "code": "1"}),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("unknown arguments"));
        assert!(validate(
            "connector.search",
            &json!({"source_id": "missing.source", "query": "wallets"})
        )
        .is_err());
        assert!(validate(
            "lumenloop.semantic",
            &json!({"query": "fees", "types": ["directory"], "limit": 5})
        )
        .is_err());
        assert!(validate(
            "lumenloop.semantic",
            &json!({"query": "fees", "types": ["articles"], "limit": 0})
        )
        .is_err());
        assert!(validate(
            "lumenloop.semantic",
            &json!({"query": "fees", "types": ["articles"], "limit": 101})
        )
        .is_err());
        assert!(validate(
            "lumenloop.semantic",
            &json!({"query": "fees", "types": ["articles"], "limit": 1.5})
        )
        .is_err());
        assert!(validate(
            "lumenloop.semantic",
            &json!({"query": "fees", "types": ["articles"], "limit": 5, "date_field": "updated_at"})
        )
        .is_err());
        assert!(validate(
            "lumenloop.semantic",
            &json!({
                "query": "fees",
                "types": ["articles"],
                "limit": 5,
                "date_start": "2026-09-21",
                "date_end": "2026-09-20"
            })
        )
        .is_err());
        assert!(validate(
            "lumenloop.semantic",
            &json!({"query": "fees", "types": ["articles"], "limit": 5, "date_start": "2026-02-29"})
        )
        .is_err());
        assert!(validate(
            "lumenloop.semantic",
            &json!({"query": " ", "types": ["articles"], "limit": 5})
        )
        .is_err());
        assert!(validate(
            "scout.projects",
            &json!({"limit": 20, "status": "inactive", "category": "Wallet"})
        )
        .is_err());
        assert!(validate("scout.projects", &json!({"limit": 20, "offset": -1})).is_err());
        assert!(validate(
            "scout.projects",
            &json!({"limit": 20, "scf_awarded": "true"})
        )
        .is_err());
    }

    #[test]
    fn validate_accepts_exact_native_filters() {
        validate(
            "connector.search",
            &json!({"source_id": "stellarlight.projects", "query": "passkey wallets"}),
        )
        .unwrap();
        validate(
            "lumenloop.semantic",
            &json!({
                "query": "account recovery",
                "types": ["articles", "research"],
                "limit": 5,
                "date_start": "2024-02-29",
                "date_end": "2026-09-21T01:00:00+02:00",
                "date_field": "publishing_date",
                "sources": ["stellar.org"]
            }),
        )
        .unwrap();
        validate(
            "lumenloop.semantic",
            &json!({
                "query": "fees",
                "types": ["articles"],
                "limit": 5,
                "date_start": "2026-09-21T03:00:00+02:00",
                "date_end": "2026-09-21T00:00:00Z"
            }),
        )
        .unwrap_err();
        validate(
            "scout.projects",
            &json!({"query": "lending", "status": "Pre-Release", "scf_awarded": false, "limit": 100, "offset": 40}),
        )
        .unwrap();
        validate("scout.projects", &json!({"limit": 1})).unwrap();
    }

    #[test]
    fn projects_url_uses_exact_native_filters() {
        let url = projects_url(Some("SCF funded"), Some("Inactive"), Some(false), 20, 40).unwrap();
        let parsed = reqwest::Url::parse(&url).unwrap();
        assert_eq!(
            parsed.origin().ascii_serialization(),
            "https://stellarlight.xyz"
        );
        assert_eq!(parsed.path(), "/api/projects/search");
        let pairs: Vec<_> = parsed
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        assert_eq!(
            pairs,
            vec![
                ("q".into(), "SCF funded".into()),
                ("status".into(), "Inactive".into()),
                ("scfAwarded".into(), "false".into()),
                ("limit".into(), "20".into()),
                ("offset".into(), "40".into()),
            ]
        );
        let bare = projects_url(None, None, None, 1, 0).unwrap();
        let bare = reqwest::Url::parse(&bare).unwrap();
        let keys: Vec<_> = bare
            .query_pairs()
            .map(|(key, _)| key.into_owned())
            .collect();
        assert_eq!(keys, vec!["limit".to_owned(), "offset".to_owned()]);
    }

    #[test]
    fn semantic_body_keeps_filters_and_detailed_text() {
        let body = semantic_body(&arguments(json!({
            "query": "account recovery",
            "types": ["articles"],
            "limit": 5,
            "date_start": "2026-01-01",
            "date_end": "2026-09-21T00:00:00.120Z",
            "date_field": "created_at",
            "sources": ["stellar.org"]
        })));
        assert_eq!(body["response_format"], "detailed");
        assert_eq!(body["query"], "account recovery");
        assert_eq!(body["types"][0], "articles");
        assert_eq!(body["limit"], 5);
        assert_eq!(body["date_start"], "2026-01-01");
        assert_eq!(body["date_field"], "created_at");
        assert_eq!(body["sources"][0], "stellar.org");
        assert!(body.get("url").is_none());
    }

    #[test]
    fn available_text_includes_the_long_summary() {
        let text = available_text(&json!({
            "title": "Recovery",
            "summary": "short summary",
            "long_summary": "full long summary",
            "similarity": 0.2
        }));
        let long = text.find("full long summary").unwrap();
        let short = text.find("short summary").unwrap();
        assert!(long < short);
        assert!(text.contains("Recovery"));
    }

    #[test]
    fn semantic_url_uses_link_when_url_is_absent() {
        let row = json!({
            "id": "9",
            "title": "Talk",
            "link": "https://example.invalid/talk",
            "summary": "A summary"
        });
        let document = semantic_document("events", &row, "raw/000000.body", &json!({}), 5).unwrap();
        assert_eq!(document.url, "https://example.invalid/talk");
        let row = json!({"id": "9", "link": {"href": "https://example.invalid/href"}});
        let document = semantic_document("events", &row, "raw/000000.body", &json!({}), 5).unwrap();
        assert_eq!(document.url, "https://example.invalid/href");
        let row = json!({
            "id": "9",
            "url": "https://example.invalid/url",
            "link": "https://example.invalid/link"
        });
        let document = semantic_document("events", &row, "raw/000000.body", &json!({}), 5).unwrap();
        assert_eq!(document.url, "https://example.invalid/url");
        assert!(document.text.contains("https://example.invalid/link"));
    }

    #[test]
    fn semantic_rows_keep_changed_text_and_skip_other_types() {
        let arguments = json!({"query": "fees", "types": ["articles"], "limit": 5});
        let payloads = vec![SemanticPayload {
            block_index: None,
            data: json!({
                "articles": [
                    {"id": 1, "title": "One", "url": "https://example.invalid/one", "summary": "alpha"},
                    {"id": 1, "title": "One", "url": "https://example.invalid/one", "summary": "alpha"},
                    {"id": 1, "title": "One revised", "url": "https://example.invalid/one-b", "summary": "beta"}
                ],
                "events": [
                    {"id": 2, "title": "Event", "url": "https://example.invalid/event", "summary": "nope"}
                ]
            }),
        }];
        let mut result = FetchResult::default();
        collect_semantic(&mut result, &arguments, &payloads, "raw/000000.body", 5, 20);
        assert_eq!(result.documents.len(), 3);
        assert_eq!(result.documents[0].id, result.documents[1].id);
        assert_eq!(result.documents[0].text, result.documents[1].text);
        assert_ne!(result.documents[2].id, result.documents[0].id);
        assert!(result.documents[2].text.contains("beta"));
        assert_eq!(result.documents[2].url, "https://example.invalid/one-b");
        assert!(result
            .documents
            .iter()
            .all(|document| document.source_id == "lumenloop.articles"));
        assert!(result.documents[2]
            .provenance
            .get("provider_document_id")
            .is_some());
    }

    #[test]
    fn multiple_blocks_keep_every_block() {
        let value = json!({
            "success": true,
            "data": {"content": [
                {"type": "text", "text": "{\"articles\":[{\"id\":1,\"title\":\"A\",\"url\":\"https://example.invalid/a\",\"summary\":\"first\"}]}"},
                {"type": "text", "text": "{\"articles\":[{\"id\":2,\"title\":\"B\",\"link\":\"https://example.invalid/b\",\"summary\":\"second\"}]}"},
                {"type": "text", "text": "not-json note"},
                {"type": "text", "text": "provider note that must remain"}
            ]},
            "meta": {"format": "blocks"}
        });
        let payloads = lumenloop_payloads(200, value).unwrap();
        assert_eq!(payloads.len(), 4);
        let arguments = json!({"query": "fees", "types": ["articles"], "limit": 5});
        let mut result = FetchResult::default();
        collect_semantic(&mut result, &arguments, &payloads, "raw/000000.body", 5, 20);
        assert_eq!(result.documents.len(), 2);
        assert_eq!(result.documents[0].url, "https://example.invalid/a");
        assert_eq!(result.documents[1].url, "https://example.invalid/b");
        let messages: Vec<_> = result
            .failures
            .iter()
            .map(|failure| failure.message.as_str())
            .collect();
        assert!(messages
            .iter()
            .any(|message| message.contains("not-json note")));
        assert!(messages
            .iter()
            .any(|message| message.contains("provider note that must remain")));
    }

    #[test]
    fn project_rows_with_changed_text_both_remain() {
        let first = json!({
            "id": "p1",
            "name": "Blend",
            "url": "https://example.invalid/a",
            "shortDescription": "one"
        });
        let repeat = first.clone();
        let changed = json!({
            "id": "p1",
            "name": "Blend",
            "url": "https://example.invalid/b",
            "shortDescription": "two"
        });
        let mut result = FetchResult::default();
        for row in [&first, &repeat, &changed] {
            store_document(
                &mut result,
                project_document(
                    row,
                    "raw/000001.body",
                    SCOUT_PROJECTS_URL,
                    &json!({}),
                    None,
                    None,
                    None,
                    20,
                    0,
                )
                .unwrap(),
            );
        }
        assert_eq!(result.documents.len(), 3);
        assert_eq!(result.documents[0].id, result.documents[1].id);
        assert_ne!(result.documents[2].id, result.documents[0].id);
        assert!(result.documents[0].text.contains("one"));
        assert!(result.documents[2].text.contains("two"));
        assert_eq!(result.documents[2].url, "https://example.invalid/b");
    }

    #[test]
    fn page_stop_keeps_a_zero_total_explicit() {
        assert_eq!(page_stop(20, 20, 20, Some(20)), PageStop::Done);
        assert_eq!(page_stop(20, 20, 20, Some(40)), PageStop::More);
        assert_eq!(page_stop(20, 20, 20, Some(0)), PageStop::UnknownTotal);
        assert_eq!(page_stop(4, 20, 4, Some(0)), PageStop::Done);
        assert_eq!(page_stop(0, 20, 10, None), PageStop::Done);
        assert_eq!(
            days_from_civil(2026, 9, 21) - days_from_civil(2026, 9, 20),
            1
        );
        assert_eq!(
            days_from_civil(2024, 3, 1) - days_from_civil(2024, 2, 28),
            2
        );
    }

    #[tokio::test]
    async fn fixture_fetches_do_not_touch_the_network() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = context(dir.path(), true, 10, 2);
        let semantic = fetch(
            &ctx,
            "lumenloop.semantic",
            &json!({
                "query": "account recovery",
                "types": ["articles", "research"],
                "limit": 5,
                "date_start": "2026-01-01",
                "sources": ["stellar.org"]
            }),
        )
        .await
        .unwrap();
        assert_eq!(semantic.documents.len(), 1);
        assert!(semantic.documents[0].text.contains("account recovery"));
        assert_eq!(
            semantic.documents[0].provenance["request_url"],
            LUMENLOOP_SEMANTIC_URL
        );
        assert_eq!(
            semantic.documents[0].provenance["request_body"]["response_format"],
            "detailed"
        );
        assert_eq!(
            semantic.documents[0].provenance["request_body"]["date_start"],
            "2026-01-01"
        );
        let projects = fetch(
            &ctx,
            "scout.projects",
            &json!({"query": "lending", "status": "Inactive", "scf_awarded": false, "limit": 20, "offset": 5}),
        )
        .await
        .unwrap();
        assert_eq!(
            projects.documents[0].provenance["request_url"],
            SCOUT_PROJECTS_URL
        );
        assert_eq!(projects.documents[0].provenance["filters"]["q"], "lending");
        assert_eq!(
            projects.documents[0].provenance["filters"]["scfAwarded"],
            false
        );
        assert_eq!(
            projects.documents[0].provenance["filters"]["status"],
            "Inactive"
        );
        assert_eq!(projects.documents[0].provenance["filters"]["offset"], 5);
        assert_eq!(raw_count(dir.path()), 0);
    }

    #[tokio::test]
    async fn connector_search_delegates_the_existing_fixture() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = context(dir.path(), true, 10, 2);
        let result = fetch(
            &ctx,
            "connector.search",
            &json!({"source_id": "stellarlight.projects", "query": "passkey wallets"}),
        )
        .await
        .unwrap();
        assert_eq!(result.documents[0].id, "stellarlight.projects:fixture");
        assert!(result.documents[0].text.contains("Offline fixture"));
        assert_eq!(raw_count(dir.path()), 0);
    }

    #[tokio::test]
    async fn zero_limits_and_bad_arguments_do_not_touch_the_network() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = context(dir.path(), false, 0, 2);
        for (operation, args) in [
            (
                "connector.search",
                json!({"source_id": "lumenloop.articles", "query": "fees"}),
            ),
            (
                "lumenloop.semantic",
                json!({"query": "fees", "types": ["articles"], "limit": 5}),
            ),
            ("scout.projects", json!({"limit": 20, "scf_awarded": true})),
        ] {
            let result = fetch(&ctx, operation, &args).await.unwrap();
            assert!(result.documents.is_empty(), "{operation}");
            assert!(!result.failures.is_empty(), "{operation}");
        }
        let ctx = context(dir.path(), false, 10, 0);
        let result = fetch(&ctx, "scout.projects", &json!({"limit": 20}))
            .await
            .unwrap();
        assert!(!result.failures.is_empty());
        let error = fetch(
            &ctx,
            "scout.projects",
            &json!({"limit": 20, "url": "https://evil.example"}),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("unknown arguments"));
        assert_eq!(raw_count(dir.path()), 0);
    }

    #[tokio::test]
    async fn semantic_without_a_key_does_not_call_the_network() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = context(dir.path(), false, 10, 2);
        let result = fetch_semantic(
            &ctx,
            &json!({"query": "fees", "types": ["articles"], "limit": 5}),
            None,
        )
        .await
        .unwrap();
        assert!(result.documents.is_empty());
        assert!(result.failures[0]
            .message
            .contains("LUMENLOOP_API_KEY is missing"));
        assert_eq!(raw_count(dir.path()), 0);
    }
}
