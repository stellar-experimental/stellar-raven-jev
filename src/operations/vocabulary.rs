//! Explicit vocabulary snapshots. No query rewriting, paging, or inferred filters.
use super::{failure, required_text, stopped, unknown};
use crate::types::{Document, FetchContext, FetchResult};
use anyhow::{bail, Result};
use reqwest::Method;
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};

const OPERATION: &str = "lumenloop.vocabulary";
const BASE: &str = "https://api.lumenloop.com/v1/tools";
const KINDS: &[&str] = &["categories", "regions", "project_tags", "content_tags"];

fn tool(kind: &str) -> &'static str {
    match kind {
        "categories" => "get_categories",
        "regions" => "get_regions",
        "project_tags" => "get_project_tags_vocabulary",
        "content_tags" => "get_tags_vocabulary",
        _ => unreachable!("Vocabulary kind was validated"),
    }
}

pub(super) fn catalog_entry() -> Value {
    json!({"operation":OPERATION,
        "arguments":{"type":"object","additionalProperties":false,"required":["kind"],
            "properties":{"kind":{"type":"string","enum":KINDS}}},
        "search_semantics":"One fixed vocabulary tool receives POST {}. No query, source, filter, limit, or offset is sent. Regions are directory values in use; categories and tags are controlled vocabularies. Project tags and content tags have separate scopes.",
        "provider_limit":null,"continuation_supported":false,
        "text_scope":"One document retains the exact UTF-8 response body, including every content block. Counts describe returned snapshots, not exhaustive coverage or durable currentness. Plain messages and malformed collections are not empty snapshots."})
}

pub(super) fn validate(map: &Map<String, Value>) -> Result<()> {
    unknown(map, &["kind"])?;
    if !KINDS.contains(&required_text(map, "kind")?.as_str()) {
        bail!("Unknown vocabulary kind");
    }
    Ok(())
}

pub(super) async fn fetch(ctx: &FetchContext, arguments: &Value) -> Result<FetchResult> {
    let kind = arguments["kind"]
        .as_str()
        .expect("Validated vocabulary kind");
    if ctx.config.fixture {
        let data = match kind {
            "regions" => {
                let mut regions = vec!["Fixture Region".to_owned(), "fixture region".to_owned()];
                regions.extend((2..97).map(|i| format!("Fixture region {i}")));
                json!({"count":97,"regions":regions})
            }
            "content_tags" => json!([{"id":"fixture","name":"Fixture content tag"}]),
            "categories" => {
                json!({"count":1,"categories":[{"id":"fixture","name":"Fixture category","slug":"fixture-category"}]})
            }
            _ => {
                json!({"count":1,"tags":[{"id":"fixture","name":"Fixture project tag","slug":"fixture-project-tag"}]})
            }
        };
        let body = serde_json::to_vec(&json!({"success":true,"data":data,"fixture":true}))?;
        return Ok(collect(kind, 200, &body, None, None));
    }
    let Some(key) = super::lumenloop_key() else {
        return Ok(stopped(OPERATION, "LUMENLOOP_API_KEY is missing."));
    };
    let url = format!("{BASE}/{}", tool(kind));
    read(ctx, kind, &url, &key).await
}

// Only fetch() supplies a production URL, selected from the fixed tool table.
async fn read(ctx: &FetchContext, kind: &str, url: &str, key: &str) -> Result<FetchResult> {
    let observed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    // HttpRecorder preserves the raw response and metadata before parsing.
    let response = match ctx
        .http
        .request(
            Method::POST,
            url,
            vec![
                ("Authorization".into(), format!("Bearer {key}")),
                ("Accept".into(), "application/json".into()),
            ],
            Some(json!({})),
        )
        .await
    {
        Ok(response) => response,
        Err(error) => {
            return Ok(stopped(
                OPERATION,
                &format!("Vocabulary read failed: {error}"),
            ))
        }
    };
    Ok(collect(
        kind,
        response.status,
        &response.body,
        Some(&response.artifact),
        Some(observed),
    ))
}

// Reject duplicate object fields before interpreting counts or collection keys.
// Arrays retain duplicates and original order; they are never normalized.
struct Unique;
impl<'de> Deserialize<'de> for Unique {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct Check;
        impl<'de> Visitor<'de> for Check {
            type Value = Unique;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("JSON with unique object fields")
            }
            fn visit_bool<E: de::Error>(self, _: bool) -> std::result::Result<Unique, E> {
                Ok(Unique)
            }
            fn visit_i64<E: de::Error>(self, _: i64) -> std::result::Result<Unique, E> {
                Ok(Unique)
            }
            fn visit_u64<E: de::Error>(self, _: u64) -> std::result::Result<Unique, E> {
                Ok(Unique)
            }
            fn visit_f64<E: de::Error>(self, _: f64) -> std::result::Result<Unique, E> {
                Ok(Unique)
            }
            fn visit_str<E: de::Error>(self, _: &str) -> std::result::Result<Unique, E> {
                Ok(Unique)
            }
            fn visit_unit<E: de::Error>(self) -> std::result::Result<Unique, E> {
                Ok(Unique)
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Unique, A::Error> {
                while seq.next_element::<Unique>()?.is_some() {}
                Ok(Unique)
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Unique, A::Error> {
                let mut names = HashSet::new();
                while let Some(name) = map.next_key::<String>()? {
                    if !names.insert(name) {
                        return Err(de::Error::custom("Duplicate JSON field"));
                    }
                    map.next_value::<Unique>()?;
                }
                Ok(Unique)
            }
        }
        deserializer.deserialize_any(Check)
    }
}

fn parse(bytes: &[u8]) -> Result<Value> {
    serde_json::from_slice::<Unique>(bytes)?;
    Ok(serde_json::from_slice(bytes)?)
}

fn rows<'a>(kind: &str, data: &'a Value) -> Option<&'a Vec<Value>> {
    match kind {
        "categories" => data.get("categories")?.as_array(),
        "regions" => data.get("regions")?.as_array(),
        "project_tags" => data.get("tags")?.as_array(),
        "content_tags" => data.as_array(),
        _ => None,
    }
}

fn valid_row(kind: &str, row: &Value) -> bool {
    let text = |v: &Value| v.as_str().is_some_and(|s| !s.trim().is_empty());
    if kind == "regions" {
        return text(row);
    }
    row.is_object()
        && (text(&row["id"]) || row["id"].as_u64().is_some())
        && text(&row["name"])
        && (kind == "content_tags" || text(&row["slug"]))
}

// Inspect response control objects, never vocabulary rows. A tag called "error"
// is data; an error marker on an envelope or parsed payload is a failed read.
fn provider_markers(value: &Value, scope: &str, artifact: &str, result: &mut FetchResult) {
    for field in [
        "__truncated",
        "error",
        "errors",
        "isError",
        "is_error",
        "success",
    ] {
        let Some(marker) = value.get(field) else {
            continue;
        };
        let flagged = match field {
            "success" => marker == &Value::Bool(false),
            "error" => !marker.is_null(),
            "errors" => {
                !marker.is_null() && marker.as_array().is_none_or(|items| !items.is_empty())
            }
            _ => !marker.is_null() && marker != &Value::Bool(false),
        };
        if flagged {
            failure(result, OPERATION, format!("Provider marker {scope}.{field} prevents a clean vocabulary snapshot. Original response retained. Artifact: {artifact}"));
        }
    }
}

fn collect(
    kind: &str,
    status: u16,
    body: &[u8],
    artifact: Option<&str>,
    observed: Option<u128>,
) -> FetchResult {
    let location = artifact.unwrap_or("offline synthetic fixture");
    let mut result = FetchResult::default();
    let problem = |result: &mut FetchResult, message: String| {
        failure(result, OPERATION, format!("{message} Artifact: {location}"));
    };
    let Ok(text) = std::str::from_utf8(body) else {
        return stopped(
            OPERATION,
            &format!("Invalid UTF-8 response. Artifact: {location}"),
        );
    };
    let value = match parse(body) {
        Ok(value) => value,
        Err(_) => {
            return stopped(
                OPERATION,
                &format!("Invalid JSON or duplicate object fields. Artifact: {location}"),
            )
        }
    };
    if !(200..300).contains(&status) || value.get("success") != Some(&Value::Bool(true)) {
        return stopped(
            OPERATION,
            &format!(
                "Vocabulary HTTP {status} or unsuccessful provider envelope. Artifact: {location}"
            ),
        );
    }
    provider_markers(&value, "envelope", location, &mut result);
    if let Some(meta) = value.get("meta") {
        provider_markers(meta, "envelope.meta", location, &mut result);
    }
    let mut payloads = Vec::new();
    if value.pointer("/meta/format").and_then(Value::as_str) == Some("blocks") {
        if let Some(wrapper) = value.get("data") {
            provider_markers(wrapper, "content_wrapper", location, &mut result);
        }
        let Some(blocks) = value.pointer("/data/content").and_then(Value::as_array) else {
            return stopped(
                OPERATION,
                &format!("Missing content blocks. Artifact: {location}"),
            );
        };
        if blocks.is_empty() {
            problem(
                &mut result,
                "No content blocks; vocabulary is unknown.".into(),
            );
        }
        for (index, block) in blocks.iter().enumerate() {
            provider_markers(block, &format!("block[{index}]"), location, &mut result);
            let data = block
                .get("text")
                .and_then(Value::as_str)
                .and_then(|t| parse(t.as_bytes()).ok());
            payloads.push((Some(index), data));
        }
    } else {
        payloads.push((None, value.get("data").cloned()));
    }
    let mut summaries = Vec::new();
    let mut valid_collections = 0;
    for (block_index, data) in payloads {
        if let Some(data) = &data {
            provider_markers(
                data,
                &format!("payload[{block_index:?}]"),
                location,
                &mut result,
            );
        }
        let mut summary = json!({"block_index":block_index,"collection_state":"unknown",
            "reported_count":data.as_ref().and_then(|d| d.get("count")),
            "reported_count_present":data.as_ref().is_some_and(|d| d.get("count").is_some()),
            "observed_count":null,"count_consistency":"unknown","vocabulary_complete":null});
        let Some(collection) = data.as_ref().and_then(|d| rows(kind, d)) else {
            problem(&mut result, format!("Block {block_index:?} has a missing or malformed {kind} collection, or a message. This is not a verified empty snapshot."));
            summaries.push(summary);
            continue;
        };
        valid_collections += 1;
        summary["collection_state"] = json!("array");
        summary["observed_count"] = json!(collection.len());
        if summary["reported_count_present"] == true {
            match summary["reported_count"].as_u64() {
                Some(count) => {
                    let matches = count == collection.len() as u64;
                    summary["count_consistency"] = json!(if matches { "match" } else { "mismatch" });
                    if !matches { problem(&mut result, format!("Block {block_index:?} reported count differs from returned array length.")); }
                }
                None => problem(&mut result, format!("Block {block_index:?} has a malformed count; the original value is retained.")),
            }
        }
        let mut seen_rows = HashSet::new();
        let mut seen_ids = HashSet::new();
        let mut invalid = Vec::new();
        let mut duplicates = Vec::new();
        let mut duplicate_ids = Vec::new();
        for (index, row) in collection.iter().enumerate() {
            if !valid_row(kind, row) {
                invalid.push(index);
            }
            if !seen_rows.insert(row.to_string()) {
                duplicates.push(index);
            }
            if kind != "regions" {
                if let Some(id) = row.get("id") {
                    if !seen_ids.insert(id.to_string()) {
                        duplicate_ids.push(index);
                    }
                }
            }
        }
        if !invalid.is_empty() || !duplicates.is_empty() || !duplicate_ids.is_empty() {
            problem(&mut result, format!("Block {block_index:?} has malformed or duplicate rows/IDs. Every row remains in the snapshot."));
        }
        summary["invalid_row_indexes"] = json!(invalid);
        summary["duplicate_row_indexes"] = json!(duplicates);
        summary["duplicate_id_indexes"] = json!(duplicate_ids);
        summaries.push(summary);
    }
    if valid_collections > 0 {
        let digest = format!("{:x}", Sha256::digest(body));
        let scope = if kind == "regions" {
            "directory_in_use_region_values"
        } else if kind == "content_tags" {
            "controlled_article_av_tags"
        } else {
            "controlled_project_vocabulary"
        };
        let state = if result.failures.is_empty() {
            "parsed"
        } else {
            "partial"
        };
        result.documents.push(Document {
            id: format!("lumenloop.vocabulary:{kind}:{digest}"),
            source_id: format!("lumenloop.vocabulary.{kind}"),
            title: format!("LumenLoop {kind} vocabulary snapshot"),
            url: format!("{BASE}/{}", tool(kind)), text: text.to_owned(),
            provenance: json!({"provider":"lumenloop","operation":OPERATION,"tool":tool(kind),
                "kind":kind,"content_scope":"provider_vocabulary_snapshot","vocabulary_scope":scope,
                "request_url":format!("{BASE}/{}",tool(kind)),"request_body":{},
                "fixture":artifact.is_none(),"http_status":status,"transport_complete":artifact.is_some(),
                "observed_at_unix_ms":observed,"observation_is_publication_date":false,
                "snapshot_status":state,"response_sha256":digest,"response_utf8_bytes":body.len(),
                "blocks":summaries,"vocabulary_complete":null,"continuation_supported":false,
                "scope_note":"All returned values are retained without case normalization or deduplication. Count agreement does not establish exhaustive coverage. This is a provider snapshot, not durable currentness or a claim about project availability."}),
            raw_artifacts: artifact.into_iter().map(str::to_owned).collect(),
        });
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{http::HttpRecorder, types::RunConfig};

    fn snapshot(kind: &str, data: Value) -> FetchResult {
        collect(
            kind,
            200,
            &serde_json::to_vec(&json!({"success":true,"data":data})).unwrap(),
            Some("raw/fixture.body"),
            Some(123),
        )
    }

    #[test]
    fn exact_kinds_and_no_guessed_arguments() {
        for (kind, expected) in [
            ("categories", "get_categories"),
            ("regions", "get_regions"),
            ("project_tags", "get_project_tags_vocabulary"),
            ("content_tags", "get_tags_vocabulary"),
        ] {
            super::super::validate(OPERATION, &json!({"kind":kind})).unwrap();
            assert_eq!(tool(kind), expected);
        }
        for arguments in [
            json!({}),
            json!({"kind":"tags"}),
            json!({"kind":null}),
            json!({"kind":"Regions"}),
            json!({"kind":"regions","source":"directory"}),
            json!({"kind":"regions","query":"x"}),
            json!({"kind":"regions","limit":1}),
            json!({"kind":"regions","url":"https://other.invalid"}),
        ] {
            assert!(super::super::validate(OPERATION, &arguments).is_err());
        }
        assert_eq!(catalog_entry()["continuation_supported"], false);
    }

    #[test]
    fn regions_preserve_exact_body_case_order_unicode_and_duplicate_rows() {
        let raw = "{\n  \"success\":true,\"data\":{\"count\":4,\"regions\":[\"Éire\",\"éire\",\"Éire\",\"日本\"],\"extra\":true}\n}";
        let result = collect(
            "regions",
            200,
            raw.as_bytes(),
            Some("raw/fixture.body"),
            Some(123),
        );
        assert_eq!(result.documents.len(), 1);
        let doc = &result.documents[0];
        assert_eq!(doc.text, raw);
        assert_eq!(doc.raw_artifacts, ["raw/fixture.body"]);
        assert_eq!(doc.url, "https://api.lumenloop.com/v1/tools/get_regions");
        let meta = &doc.provenance;
        assert_eq!(meta["vocabulary_scope"], "directory_in_use_region_values");
        assert_eq!(meta["blocks"][0]["duplicate_row_indexes"], json!([2]));
        assert_eq!(meta["blocks"][0]["observed_count"], 4);
        assert_eq!(meta["blocks"][0]["count_consistency"], "match");
        assert_eq!(meta["vocabulary_complete"], Value::Null);
        assert_eq!(meta["snapshot_status"], "partial");
        assert!(!result.failures.is_empty());
    }

    #[test]
    fn content_and_project_tag_shapes_are_not_interchanged() {
        let content = json!([{"id":1,"name":"Payment"}]);
        let result = snapshot("content_tags", content.clone());
        assert_eq!(result.documents.len(), 1);
        assert!(result.failures.is_empty());
        assert_eq!(
            result.documents[0].provenance["blocks"][0]["reported_count_present"],
            false
        );
        assert_eq!(
            result.documents[0].provenance["blocks"][0]["count_consistency"],
            "unknown"
        );
        assert!(snapshot("project_tags", content).documents.is_empty());
        assert!(snapshot(
            "content_tags",
            json!({"count":1,"tags":[{"id":1,"name":"Payment","slug":"payment"}]})
        )
        .documents
        .is_empty());
        let project = snapshot(
            "project_tags",
            json!({"count":1,"tags":[{"id":1,"name":"Payment","slug":"payment","other":[1,2]}]}),
        );
        assert!(project.failures.is_empty());
        assert!(project.documents[0].text.contains("other"));
    }

    #[test]
    fn valid_empty_arrays_are_not_missing_or_message_responses() {
        for (kind, data) in [
            ("categories", json!({"count":0,"categories":[]})),
            ("regions", json!({"count":0,"regions":[]})),
            ("project_tags", json!({"count":0,"tags":[]})),
            ("content_tags", json!([])),
        ] {
            let result = snapshot(kind, data);
            assert_eq!(result.documents.len(), 1);
            assert!(result.failures.is_empty());
            assert_eq!(
                result.documents[0].provenance["blocks"][0]["observed_count"],
                0
            );
            assert_eq!(
                result.documents[0].provenance["vocabulary_complete"],
                Value::Null
            );
        }
        for data in [
            Value::Null,
            json!({}),
            json!({"text":"No results"}),
            json!({"count":0}),
            json!({"count":0,"regions":null}),
            json!({"regions":"No results"}),
        ] {
            let result = snapshot("regions", data);
            assert!(result.documents.is_empty());
            assert!(!result.failures.is_empty());
        }
    }

    #[test]
    fn malformed_counts_and_rows_remain_visible_with_explicit_failures() {
        for count in [
            Value::Null,
            json!("2"),
            json!(-1),
            json!(true),
            json!({"unexpected":2}),
            json!(9),
        ] {
            let result = snapshot(
                "categories",
                json!({"count":count,"categories":[
                {"id":1,"name":"Payment","slug":"payment"},{"id":1,"name":"Changed"},null]}),
            );
            assert_eq!(result.documents.len(), 1);
            let doc = &result.documents[0];
            let original: Value = serde_json::from_str(&doc.text).unwrap();
            assert_eq!(original["data"]["count"], count);
            assert_eq!(original["data"]["categories"].as_array().unwrap().len(), 3);
            assert_eq!(
                doc.provenance["blocks"][0]["invalid_row_indexes"],
                json!([1, 2])
            );
            assert_eq!(
                doc.provenance["blocks"][0]["duplicate_id_indexes"],
                json!([1])
            );
            assert_eq!(doc.provenance["blocks"][0]["reported_count"], count);
            assert_eq!(doc.provenance["snapshot_status"], "partial");
            assert!(!result.failures.is_empty());
        }
    }

    #[test]
    fn blocks_preserve_every_payload_and_never_sum_ambiguous_snapshots() {
        let envelope = json!({"success":true,"meta":{"format":"blocks"},"data":{"content":[
            {"type":"text","text":"{\"count\":1,\"regions\":[\"North\"]}"},
            {"type":"text","text":"Not a collection"},
            {"type":"image","data":"opaque"},
            {"type":"text","text":"{\"regions\":[\"South\"]}"},
            {"type":"text","text":"{\"count\":0,\"count\":1,\"regions\":[]}"}
        ]}});
        let raw = serde_json::to_vec(&envelope).unwrap();
        let result = collect("regions", 200, &raw, Some("raw/all.body"), Some(123));
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].text.as_bytes(), raw);
        let blocks = &result.documents[0].provenance["blocks"];
        assert_eq!(blocks.as_array().unwrap().len(), 5);
        assert_eq!(blocks[0]["observed_count"], 1);
        assert_eq!(blocks[3]["observed_count"], 1);
        for i in [1, 2, 4] {
            assert_eq!(blocks[i]["observed_count"], Value::Null);
        }
        assert_eq!(result.failures.len(), 3);
    }

    #[test]
    fn duplicate_fields_invalid_json_and_http_errors_never_become_empty_facts() {
        for body in [
            b"not json".as_slice(),
            b"{\"success\":true,\"success\":false,\"data\":[]}",
            b"{\"success\":true,\"data\":{\"count\":1,\"count\":0,\"regions\":[]}}",
            b"\xff",
        ] {
            let result = collect("regions", 200, body, Some("raw/error.body"), Some(123));
            assert!(result.documents.is_empty());
            assert!(result.failures[0].message.contains("raw/error.body"));
        }
        for status in [401, 402, 403, 429, 500] {
            let result = collect(
                "regions",
                status,
                b"{\"success\":true,\"data\":{\"count\":0,\"regions\":[]}}",
                Some("raw/error.body"),
                Some(123),
            );
            assert!(result.documents.is_empty());
            assert!(result.failures[0]
                .message
                .contains(&format!("HTTP {status}")));
        }
    }

    #[test]
    fn truncation_and_error_markers_are_partial_even_when_counts_match() {
        let data = json!({"count":1,"regions":["North"]});
        for marker in [
            json!({"__truncated":true}),
            json!({"__truncated":"true"}),
            json!({"error":"upstream failure"}),
            json!({"errors":[{"message":"failure"}]}),
            json!({"isError":true}),
            json!({"is_error":true}),
        ] {
            for scope in ["envelope", "meta", "wrapper", "block", "payload"] {
                let mut payload = data.clone();
                if scope == "payload" {
                    payload
                        .as_object_mut()
                        .unwrap()
                        .extend(marker.as_object().unwrap().clone());
                }
                let mut envelope = json!({"success":true,"meta":{"format":"blocks"},"data":{"content":[
                    {"type":"text","text":serde_json::to_string(&payload).unwrap()}
                ]}});
                let target = match scope {
                    "envelope" => Some(&mut envelope),
                    "meta" => envelope.get_mut("meta"),
                    "wrapper" => envelope.get_mut("data"),
                    "block" => envelope.pointer_mut("/data/content/0"),
                    _ => None,
                };
                if let Some(target) = target {
                    target
                        .as_object_mut()
                        .unwrap()
                        .extend(marker.as_object().unwrap().clone());
                }
                let raw = serde_json::to_vec(&envelope).unwrap();
                let result = collect("regions", 200, &raw, Some("raw/marked.body"), Some(123));
                assert_eq!(result.documents.len(), 1, "{scope}: {marker}");
                assert_eq!(result.documents[0].text.as_bytes(), raw);
                assert_eq!(
                    result.documents[0].provenance["blocks"][0]["count_consistency"],
                    "match"
                );
                assert_eq!(result.documents[0].provenance["snapshot_status"], "partial");
                assert!(
                    result
                        .failures
                        .iter()
                        .any(|f| f.message.contains("Provider marker")),
                    "{scope}: {marker}"
                );
            }
        }
        let direct = snapshot(
            "regions",
            json!({"count":1,"regions":["North"],"__truncated":true}),
        );
        assert_eq!(direct.documents[0].provenance["snapshot_status"], "partial");
        assert!(!direct.failures.is_empty());
    }

    #[test]
    fn benign_markers_and_tag_row_fields_do_not_create_response_errors() {
        let data = json!({"count":1,"categories":[{"id":1,"name":"error","slug":"error", "error":"row data", "__truncated":true}],
            "__truncated":false,"error":null,"errors":[],"isError":false,"is_error":null});
        let result = snapshot("categories", data);
        assert!(result.failures.is_empty());
        assert_eq!(result.documents[0].provenance["snapshot_status"], "parsed");
        assert!(result.documents[0].text.contains("row data"));
        let nested_failure = snapshot(
            "regions",
            json!({"count":1,"regions":["North"],"success":false}),
        );
        assert_eq!(
            nested_failure.documents[0].provenance["snapshot_status"],
            "partial"
        );
    }

    #[tokio::test]
    async fn every_fixture_kind_and_zero_limits_stay_offline() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = RunConfig {
            fixture: true,
            output_dir: temp.path().to_path_buf(),
            max_documents: 1,
            ..RunConfig::default()
        };
        let http = HttpRecorder::new(temp.path(), &config).unwrap();
        for kind in KINDS {
            let result = super::super::fetch(
                &FetchContext {
                    config: config.clone(),
                    http: http.clone(),
                },
                OPERATION,
                &json!({"kind":kind}),
            )
            .await
            .unwrap();
            assert_eq!(result.documents.len(), 1);
            assert_eq!(result.documents[0].provenance["fixture"], true);
            assert_eq!(
                result.documents[0].provenance["observed_at_unix_ms"],
                Value::Null
            );
            assert!(result.documents[0].raw_artifacts.is_empty());
            if *kind == "regions" {
                let response: Value = serde_json::from_str(&result.documents[0].text).unwrap();
                assert_eq!(response["data"]["regions"].as_array().unwrap().len(), 97);
                assert_eq!(response["data"]["regions"][0], "Fixture Region");
                assert_eq!(response["data"]["regions"][1], "fixture region");
                assert_eq!(
                    result.documents[0].provenance["blocks"][0]["observed_count"],
                    97
                );
            }
        }
        config.max_documents = 0;
        let result = super::super::fetch(
            &FetchContext { config, http },
            OPERATION,
            &json!({"kind":"regions"}),
        )
        .await
        .unwrap();
        assert!(result.documents.is_empty());
        assert_eq!(
            std::fs::read_dir(temp.path().join("raw")).unwrap().count(),
            0
        );
    }

    #[tokio::test]
    async fn http_error_is_saved_before_parse_and_no_retry_occurs() {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };
        let temp = tempfile::tempdir().unwrap();
        let config = RunConfig {
            fixture: false,
            output_dir: temp.path().to_path_buf(),
            timeout_secs: 2,
            ..RunConfig::default()
        };
        let http = HttpRecorder::loopback_for_test(temp.path(), &config).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/fixture", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut part = [0; 4096];
                let n = socket.read(&mut part).await.unwrap();
                assert!(n > 0);
                request.extend_from_slice(&part[..n]);
                if request.ends_with(b"\r\n\r\n{}") {
                    break;
                }
                assert!(request.len() < 8192);
            }
            assert!(request.starts_with(b"POST /fixture HTTP/1.1"));
            socket.write_all(b"HTTP/1.1 429 Slow Down\r\nContent-Length: 8\r\nConnection: close\r\n\r\nnot-json").await.unwrap();
            socket.shutdown().await.unwrap();
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(40), listener.accept())
                    .await
                    .is_err()
            );
        });
        let result = read(
            &FetchContext { config, http },
            "regions",
            &url,
            "fixture-key",
        )
        .await
        .unwrap();
        server.await.unwrap();
        assert!(result.documents.is_empty());
        assert_eq!(
            std::fs::read(temp.path().join("raw/000000.body")).unwrap(),
            b"not-json"
        );
        let meta: Value =
            serde_json::from_slice(&std::fs::read(temp.path().join("raw/000000.json")).unwrap())
                .unwrap();
        assert_eq!(meta["status"], 429);
        assert_eq!(meta["request_body"], json!({}));
        assert_eq!(meta["request_headers"]["Authorization"], "[REDACTED]");
        assert_eq!(
            std::fs::read_dir(temp.path().join("raw")).unwrap().count(),
            2
        );
    }
}
