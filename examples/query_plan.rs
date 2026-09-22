//! Agent-authored retrieval plans.
//!
//! The plan keeps the original question. Each call names one known source, one search
//! question, one reason, and its own document and page caps. `connectors::fetch` receives
//! that search question. Connector code can change the question before HTTP. This example
//! records that rule and does not observe the wire query.
//!
//! Scores use the original question and one `JevClient`. Fixture mode and dry-run make no
//! network call. A live run with a zero spend cap stops before a request.

use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use stellar_raven_jev::connectors;
use stellar_raven_jev::http::HttpRecorder;
use stellar_raven_jev::jev::JevClient;
use stellar_raven_jev::types::{Document, Failure, FetchContext, FetchResult, RunConfig, Source};

const SCHEMA_VERSION: u64 = 1;
const HARD_MAX_CALLS: u64 = 32;
const HARD_MAX_DOCUMENTS: u64 = 200;
const HARD_MAX_PAGES: u64 = 64;
const HARD_MAX_BODY_BYTES: u64 = 8 * 1024 * 1024;
const HARD_MAX_SPEND_USD: f64 = 100.0;
const MAX_QUESTION_BYTES: usize = 8_000;
const MAX_QUERY_BYTES: usize = 2_000;
const MAX_REASON_BYTES: usize = 500;
const MAX_SOURCE_ID_BYTES: usize = 200;
const MAX_PLAN_BYTES: usize = 1_000_000;
const HTTP_TIMEOUT_SECS: u64 = 30;

#[derive(Clone, Debug)]
struct Bounds {
    max_calls: usize,
    max_documents: usize,
    max_pages: usize,
    max_body_bytes: usize,
    max_spend_usd: f64,
}

#[derive(Clone, Debug)]
struct Call {
    source_id: String,
    query: String,
    reason: String,
    max_documents: usize,
    max_pages: usize,
}

#[derive(Clone, Debug)]
struct Plan {
    question: String,
    bounds: Bounds,
    calls: Vec<Call>,
}

#[derive(Debug)]
struct Transform {
    changes_query: bool,
    summary: &'static str,
    planner_role: &'static str,
}

#[derive(Clone, Debug)]
struct Kept {
    call_index: usize,
    query: String,
    reason: String,
    document: Document,
}

struct Classified {
    admitted: Vec<Kept>,
    duplicates: Vec<Value>,
    omitted: Vec<Value>,
}

struct FetchedCall {
    call: Call,
    result: FetchResult,
}

fn limitations() -> Vec<&'static str> {
    vec![
        "Each planned query goes to connectors::fetch. The connectors own the request URLs.",
        "A connector can change the planned query before HTTP. This artifact stores the planned query and the known rule.",
        "Document scores use the original question.",
        "One Jev client scores every admitted document. The plan selects the sources.",
        "Identical source, title, URL, and text receive one score. Changed content receives another score. Copies retain full provenance.",
        "Each call has its own document cap and page cap.",
        "HttpRecorder applies max_body_bytes to each response body.",
        "Fixture scores are fixed offline values. Dry-run and fixture mode make no network call.",
        "Live mode needs a spend cap above zero. A zero cap stops the run before a request.",
    ]
}

fn sha256_hex(data: &[u8]) -> String {
    fn rotr(value: u32, bits: u32) -> u32 {
        value.rotate_right(bits)
    }
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let bit_len = (data.len() as u128).saturating_mul(8).min(u64::MAX as u128) as u64;
    let mut message = data.to_vec();
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_len.to_be_bytes());
    let mut hash: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    for chunk in message.chunks(64) {
        let mut words = [0u32; 64];
        for (index, word) in words.iter_mut().enumerate().take(16) {
            let start = index * 4;
            *word = u32::from_be_bytes(chunk[start..start + 4].try_into().unwrap());
        }
        for index in 16..64 {
            let small =
                rotr(words[index - 15], 7) ^ rotr(words[index - 15], 18) ^ (words[index - 15] >> 3);
            let large =
                rotr(words[index - 2], 17) ^ rotr(words[index - 2], 19) ^ (words[index - 2] >> 10);
            words[index] = words[index - 16]
                .wrapping_add(small)
                .wrapping_add(words[index - 7])
                .wrapping_add(large);
        }
        let mut a = hash[0];
        let mut b = hash[1];
        let mut c = hash[2];
        let mut d = hash[3];
        let mut e = hash[4];
        let mut f = hash[5];
        let mut g = hash[6];
        let mut h = hash[7];
        for index in 0..64 {
            let s1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
            let choose = (e & f) ^ ((!e) & g);
            let t1 = h
                .wrapping_add(s1)
                .wrapping_add(choose)
                .wrapping_add(K[index])
                .wrapping_add(words[index]);
            let s0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(majority);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        hash[0] = hash[0].wrapping_add(a);
        hash[1] = hash[1].wrapping_add(b);
        hash[2] = hash[2].wrapping_add(c);
        hash[3] = hash[3].wrapping_add(d);
        hash[4] = hash[4].wrapping_add(e);
        hash[5] = hash[5].wrapping_add(f);
        hash[6] = hash[6].wrapping_add(g);
        hash[7] = hash[7].wrapping_add(h);
    }
    hash.iter().map(|word| format!("{word:08x}")).collect()
}

fn source_catalog() -> BTreeMap<String, Source> {
    let mut catalog = BTreeMap::new();
    for source in connectors::sources() {
        catalog.entry(source.id.clone()).or_insert(source);
    }
    catalog
}

fn transform_for(source: &Source) -> Transform {
    let id = source.id.as_str();
    if source.family == "lumenloop" {
        return Transform {
            changes_query: id == "lumenloop.jobs",
            summary: if id == "lumenloop.jobs" {
                "Jobs search uses a quoted phrase, a known role token, or the full question. The request query can differ."
            } else {
                "This LumenLoop tool sends the planned query as its query field."
            },
            planner_role:
                "The planner record is for comparison. LumenLoop uses its own query text.",
        };
    }
    if source.family == "algolia" {
        return Transform {
            changes_query: true,
            summary: "Algolia turns the planned query into keyword text. Docs can send one facet query per page. Site search can retry a shorter query.",
            planner_role: "Algolia uses this planner to build the wire query.",
        };
    }
    if id == "stellarlight.builds" {
        return Transform {
            changes_query: true,
            summary: "Builds send only the first keyword variant. No keyword variant means no search request.",
            planner_role: "Builds use the first keyword variant from this planner.",
        };
    }
    if matches!(
        id,
        "stellarlight.skills" | "stellarlight.rwa" | "stellarlight.stablecoins"
    ) {
        return Transform {
            changes_query: true,
            summary: "This source does not send the planned query. It reads a fixed catalog.",
            planner_role:
                "The planner record is diagnostic. Scout request text comes from Scout rules.",
        };
    }
    if id.starts_with("stellarlight.research.") {
        return Transform {
            changes_query: false,
            summary: "Research search sends the planned query as q. The source id selects the collection.",
            planner_role: "The planner record is diagnostic. Scout request text comes from Scout rules.",
        };
    }
    if matches!(
        id,
        "stellarlight.projects" | "stellarlight.repos" | "stellarlight.partners"
    ) {
        return Transform {
            changes_query: false,
            summary: "This listing sends the planned query as q.",
            planner_role:
                "The planner record is diagnostic. Scout request text comes from Scout rules.",
        };
    }
    if source.family == "stellarlight" {
        return Transform {
            changes_query: true,
            summary: "Scout can drop collection words and retry one original term. The request query can differ.",
            planner_role: "The planner record is diagnostic. Scout request text comes from Scout rules.",
        };
    }
    Transform {
        changes_query: true,
        summary: "This source family has no recorded query rule.",
        planner_role: "The planner record is diagnostic.",
    }
}

fn shared_planner(query: &str) -> Value {
    match serde_json::from_str::<Value>(&stellar_raven_jev::query::plan(query).to_json()) {
        Ok(value) => value,
        Err(_) => json!({"planner": "deterministic-v1", "parse_error": true}),
    }
}

fn transform_record(call_index: usize, source: &Source, query: &str) -> Value {
    let transform = transform_for(source);
    json!({
        "call_index": call_index,
        "source_id": source.id,
        "family": source.family,
        "planned_query": query,
        "planned_query_is_fetch_argument": true,
        "connector_can_change_query": transform.changes_query,
        "wire_query_observed": false,
        "summary": transform.summary,
        "shared_planner_role": transform.planner_role,
        "shared_planner": shared_planner(query),
    })
}

fn allowed_keys(value: &Value, allowed: &[&str], label: &str) -> Result<(), String> {
    let Some(object) = value.as_object() else {
        return Err(format!("The {label} must be a JSON object."));
    };
    for key in object.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(format!("The plan field {key} is not allowed."));
        }
    }
    Ok(())
}

fn require_nonempty(label: &str, text: &str, max_bytes: usize) -> Result<(), String> {
    if text.trim().is_empty() {
        return Err(format!("The {label} is empty."));
    }
    if text.len() > max_bytes {
        return Err(format!("The {label} exceeds {max_bytes} bytes."));
    }
    // Questions and queries are inert data. Only source IDs select network destinations.
    Ok(())
}

fn bound_count(name: &str, value: u64, hard_max: u64) -> Result<usize, String> {
    if value == 0 {
        return Err(format!("The {name} bound must be above zero."));
    }
    if value > hard_max {
        return Err(format!("The {name} bound exceeds {hard_max}."));
    }
    Ok(value as usize)
}

fn parse_plan(value: &Value) -> Result<Plan, String> {
    allowed_keys(
        value,
        &["schema_version", "question", "bounds", "calls"],
        "plan",
    )?;
    let bounds_value = value
        .get("bounds")
        .ok_or("The plan field bounds is required.")?;
    allowed_keys(
        bounds_value,
        &[
            "max_calls",
            "max_documents",
            "max_pages",
            "max_body_bytes",
            "max_spend_usd",
        ],
        "bounds object",
    )?;
    let calls_value = value
        .get("calls")
        .ok_or("The plan field calls is required.")?;
    let Some(calls_array) = calls_value.as_array() else {
        return Err("The calls field must be a JSON array.".into());
    };
    for call in calls_array {
        allowed_keys(
            call,
            &["source_id", "query", "reason", "max_documents", "max_pages"],
            "call object",
        )?;
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct RawBounds {
        max_calls: u64,
        max_documents: u64,
        max_pages: u64,
        max_body_bytes: u64,
        max_spend_usd: f64,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct RawCall {
        source_id: String,
        query: String,
        reason: String,
        max_documents: u64,
        max_pages: u64,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct RawPlan {
        schema_version: u64,
        question: String,
        bounds: RawBounds,
        calls: Vec<RawCall>,
    }

    let raw: RawPlan = serde_json::from_value(value.clone())
        .map_err(|_| "The plan JSON does not match the schema.".to_owned())?;
    if raw.schema_version != SCHEMA_VERSION {
        return Err("The plan schema version must be 1.".into());
    }
    require_nonempty("question", &raw.question, MAX_QUESTION_BYTES)?;
    let bounds = Bounds {
        max_calls: bound_count("call", raw.bounds.max_calls, HARD_MAX_CALLS)?,
        max_documents: bound_count("document", raw.bounds.max_documents, HARD_MAX_DOCUMENTS)?,
        max_pages: bound_count("page", raw.bounds.max_pages, HARD_MAX_PAGES)?,
        max_body_bytes: bound_count("body byte", raw.bounds.max_body_bytes, HARD_MAX_BODY_BYTES)?,
        max_spend_usd: raw.bounds.max_spend_usd,
    };
    if !bounds.max_spend_usd.is_finite() || bounds.max_spend_usd < 0.0 {
        return Err("The spend cap must be a finite number from 0 to 100.".into());
    }
    if bounds.max_spend_usd > HARD_MAX_SPEND_USD {
        return Err("The spend cap exceeds 100 USD.".into());
    }
    if raw.calls.is_empty() {
        return Err("The plan needs at least one call.".into());
    }
    if raw.calls.len() > bounds.max_calls {
        return Err("The call count exceeds the call bound.".into());
    }

    let catalog = source_catalog();
    let mut calls = Vec::with_capacity(raw.calls.len());
    let mut document_total: u64 = 0;
    let mut page_total: u64 = 0;
    for raw_call in raw.calls {
        require_nonempty("query", &raw_call.query, MAX_QUERY_BYTES)?;
        require_nonempty("reason", &raw_call.reason, MAX_REASON_BYTES)?;
        if raw_call.source_id.trim().is_empty() || raw_call.source_id.len() > MAX_SOURCE_ID_BYTES {
            return Err("The source id is empty or too long.".into());
        }
        if !catalog.contains_key(&raw_call.source_id) {
            return Err(format!("The source id is unknown: {}.", raw_call.source_id));
        }
        if raw_call.max_documents == 0 || raw_call.max_documents > bounds.max_documents as u64 {
            return Err("A call document cap must fit inside the document bound.".into());
        }
        if raw_call.max_pages == 0 || raw_call.max_pages > bounds.max_pages as u64 {
            return Err("A call page cap must fit inside the page bound.".into());
        }
        document_total = document_total
            .checked_add(raw_call.max_documents)
            .ok_or("The document total exceeds the document bound.")?;
        page_total = page_total
            .checked_add(raw_call.max_pages)
            .ok_or("The page total exceeds the page bound.")?;
        calls.push(Call {
            source_id: raw_call.source_id,
            query: raw_call.query,
            reason: raw_call.reason,
            max_documents: raw_call.max_documents as usize,
            max_pages: raw_call.max_pages as usize,
        });
    }
    if document_total > bounds.max_documents as u64 {
        return Err("The document total exceeds the document bound.".into());
    }
    if page_total > bounds.max_pages as u64 {
        return Err("The page total exceeds the page bound.".into());
    }
    Ok(Plan {
        question: raw.question,
        bounds,
        calls,
    })
}

fn read_plan_value(path: &Path) -> Result<Value, String> {
    let bytes = std::fs::read(path)
        .map_err(|_| format!("Cannot read the plan file {}.", path.display()))?;
    if bytes.len() > MAX_PLAN_BYTES {
        return Err("The plan file exceeds 1000000 bytes.".into());
    }
    serde_json::from_slice(&bytes).map_err(|_| "The plan file is not valid JSON.".to_owned())
}

fn load_plan(path: &Path) -> Result<Plan, String> {
    parse_plan(&read_plan_value(path)?)
}

fn dry_run_report(plan: &Plan) -> Value {
    let catalog = source_catalog();
    let calls: Vec<_> = plan
        .calls
        .iter()
        .enumerate()
        .map(|(index, call)| {
            let source = &catalog[&call.source_id];
            json!({
                "call_index": index,
                "source_id": call.source_id,
                "family": source.family,
                "query": call.query,
                "reason": call.reason,
                "max_documents": call.max_documents,
                "max_pages": call.max_pages,
                "transform": transform_record(index, source, &call.query),
            })
        })
        .collect();
    json!({
        "schema_version": SCHEMA_VERSION,
        "status": "dry-run",
        "ok": true,
        "mode": "dry-run",
        "question": plan.question,
        "scoring_question": plan.question,
        "bounds": bounds_json(&plan.bounds),
        "calls": calls,
        "fetch_performed": false,
        "jev_client_constructed": false,
        "paid_jev_call": false,
        "limitations": limitations(),
    })
}

fn bounds_json(bounds: &Bounds) -> Value {
    json!({
        "max_calls": bounds.max_calls,
        "max_documents": bounds.max_documents,
        "max_pages": bounds.max_pages,
        "max_body_bytes": bounds.max_body_bytes,
        "max_spend_usd": bounds.max_spend_usd,
    })
}

fn scoring_id(document: &Document) -> String {
    format!(
        "{}::{}",
        document.source_id,
        sha256_hex(
            &serde_json::to_vec(&(
                &document.source_id,
                &document.title,
                &document.url,
                &document.text
            ))
            .unwrap()
        )
    )
}

fn copy_json(call_index: usize, query: &str, reason: &str, document: &Document) -> Value {
    json!({
        "call_index": call_index,
        "query": query,
        "reason": reason,
        "scoring_document_id": scoring_id(document),
        "content_sha256": sha256_hex(document.text.as_bytes()),
        "document": document,
    })
}

fn omit(
    reason_code: &str,
    message: &str,
    call_index: usize,
    query: &str,
    reason: &str,
    document: &Document,
    related: Option<String>,
) -> Value {
    json!({
        "reason_code": reason_code,
        "message": message,
        "call_index": call_index,
        "query": query,
        "reason": reason,
        "related_scoring_document_id": related,
        "document": document,
    })
}

fn classify(calls: &[FetchedCall], max_documents: usize) -> Classified {
    let mut admitted: Vec<Kept> = Vec::new();
    let mut omitted = Vec::new();
    let mut groups: BTreeMap<String, Value> = BTreeMap::new();
    let mut owners: BTreeMap<String, usize> = BTreeMap::new();
    for (call_index, fetched) in calls.iter().enumerate() {
        for document in &fetched.result.documents {
            let key = scoring_id(document);
            let copy = copy_json(
                call_index,
                &fetched.call.query,
                &fetched.call.reason,
                document,
            );
            let group = groups.entry(key.clone()).or_insert_with(|| {
                json!({
                    "match":"source_title_url_text", "scoring_document_id":key,
                    "content_sha256":sha256_hex(document.text.as_bytes()), "copies":[]
                })
            });
            group["copies"].as_array_mut().unwrap().push(copy);
            if let Some(&owner) = owners.get(&key) {
                let kind = if admitted[owner].document.id == document.id {
                    "duplicate"
                } else {
                    "content_duplicate"
                };
                omitted.push(omit(
                    kind,
                    "An identical source, title, URL, and text already has a scoring record.",
                    call_index,
                    &fetched.call.query,
                    &fetched.call.reason,
                    document,
                    Some(key),
                ));
            } else if document.text.trim().is_empty() {
                omitted.push(omit(
                    "empty_text",
                    "The document has no text. It was not scored.",
                    call_index,
                    &fetched.call.query,
                    &fetched.call.reason,
                    document,
                    None,
                ));
            } else if admitted.len() >= max_documents {
                omitted.push(omit(
                    "document_limit",
                    "The document cap stopped scoring. Full text remains saved.",
                    call_index,
                    &fetched.call.query,
                    &fetched.call.reason,
                    document,
                    None,
                ));
            } else {
                owners.insert(key, admitted.len());
                admitted.push(Kept {
                    call_index,
                    query: fetched.call.query.clone(),
                    reason: fetched.call.reason.clone(),
                    document: document.clone(),
                });
            }
        }
    }
    Classified {
        admitted,
        omitted,
        duplicates: groups
            .into_values()
            .filter(|g| g["copies"].as_array().unwrap().len() > 1)
            .collect(),
    }
}

fn base_config(output: &Path, plan: &Plan, fixture: bool) -> RunConfig {
    RunConfig {
        fixture,
        output_dir: output.to_path_buf(),
        budget_usd: plan.bounds.max_spend_usd,
        timeout_secs: HTTP_TIMEOUT_SECS,
        concurrency: 4,
        max_pages: 1,
        max_documents: 1,
        per_source_documents: 1,
        fetch_deadline_secs: 10,
        max_body_bytes: plan.bounds.max_body_bytes,
        route_passes: 1,
        source_threshold: 0.2,
        document_threshold: 0.4,
        uncertain_threshold: 0.15,
    }
}

fn write_json(path: &Path, value: &Value) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|_| format!("Cannot create {}.", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|_| format!("Cannot encode {}.", path.display()))?;
    let temporary = path.with_extension("json.tmp");
    std::fs::write(&temporary, bytes).map_err(|_| format!("Cannot write {}.", path.display()))?;
    std::fs::rename(temporary, path).map_err(|_| format!("Cannot save {}.", path.display()))
}

fn restrict_dir(path: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(|_| "Cannot set the output directory permissions.".to_owned())?;
    }
    let _ = path;
    Ok(())
}

fn artifact_list(root: &Path, manifest_bytes: u64) -> Result<Vec<Value>, String> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let entries =
            std::fs::read_dir(&dir).map_err(|_| format!("Cannot read {}.", dir.display()))?;
        for entry in entries {
            let entry = entry.map_err(|_| format!("Cannot read {}.", dir.display()))?;
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            let relative = path
                .strip_prefix(root)
                .map_err(|_| "Cannot build an artifact path.".to_owned())?;
            let bytes = if relative == Path::new("manifest.json") {
                manifest_bytes
            } else {
                entry
                    .metadata()
                    .map_err(|_| format!("Cannot read {}.", path.display()))?
                    .len()
            };
            files.push(json!({
                "path": relative.to_string_lossy(),
                "bytes": bytes,
            }));
        }
    }
    if !files.iter().any(|file| file["path"] == "manifest.json") {
        files.push(json!({"path": "manifest.json", "bytes": manifest_bytes}));
    }
    files.sort_by(|left, right| left["path"].as_str().cmp(&right["path"].as_str()));
    Ok(files)
}

fn count_raw_bodies(root: &Path) -> Result<usize, String> {
    let raw = root.join("raw");
    if !raw.is_dir() {
        return Ok(0);
    }
    let mut count = 0;
    for entry in std::fs::read_dir(&raw).map_err(|_| "Cannot read the raw directory.".to_owned())? {
        let entry = entry.map_err(|_| "Cannot read the raw directory.".to_owned())?;
        if entry.path().extension().and_then(|ext| ext.to_str()) == Some("body") {
            count += 1;
        }
    }
    Ok(count)
}

fn budget_stopped(message: &str) -> bool {
    message.contains("Jev budget") || message.contains("Jev stopped")
}

async fn execute(plan: &Plan, output: &Path, fixture: bool) -> Result<Value, String> {
    if !plan.bounds.max_spend_usd.is_finite()
        || plan.bounds.max_spend_usd < 0.0
        || plan.bounds.max_spend_usd > HARD_MAX_SPEND_USD
    {
        return Err("The spend cap must be a finite number from 0 to 100.".into());
    }
    if !fixture && plan.bounds.max_spend_usd <= 0.0 {
        return Err("Live mode needs a spend cap above zero.".into());
    }

    if let Some(parent) = output.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).map_err(|_| "Cannot create output parent.".to_owned())?;
    }
    std::fs::create_dir(output).map_err(|_| {
        format!(
            "Output directory already exists or cannot be created: {}.",
            output.display()
        )
    })?;
    restrict_dir(output)?;
    let mut failures = Vec::new();
    write_json(
        &output.join("plan.json"),
        &json!({
            "schema_version": SCHEMA_VERSION,
            "question": plan.question,
            "bounds": bounds_json(&plan.bounds),
            "calls": plan.calls.iter().map(|call| json!({
                "source_id": call.source_id,
                "query": call.query,
                "reason": call.reason,
                "max_documents": call.max_documents,
                "max_pages": call.max_pages,
            })).collect::<Vec<_>>(),
        }),
    )?;

    let catalog = source_catalog();
    let config = base_config(output, plan, fixture);
    let http = HttpRecorder::new(output, &config).map_err(|error| error.to_string())?;
    let client = match JevClient::new(&config, &http) {
        Ok(client) => client,
        Err(error) => {
            failures.push(Failure {
                stage: "jev".into(),
                source_id: None,
                message: error.to_string(),
            });
            let manifest = failed_manifest(output, plan, fixture, &failures, None)?;
            return Ok(manifest);
        }
    };

    write_json(&output.join("usage.json"), &usage_json(&client.usage()))?;
    write_json(
        &output.join("manifest.json"),
        &json!({"status":"running", "question":plan.question,
        "bounds":bounds_json(&plan.bounds), "usage":client.usage()}),
    )?;
    let mut fetched = Vec::new();
    let mut call_records = Vec::new();
    let mut transforms = Vec::new();
    for (index, call) in plan.calls.iter().enumerate() {
        let source = catalog[&call.source_id].clone();
        transforms.push(transform_record(index, &source, &call.query));
        let mut call_config = config.clone();
        call_config.max_documents = call.max_documents;
        call_config.per_source_documents = call.max_documents;
        call_config.max_pages = call.max_pages;
        let ctx = FetchContext {
            http: http.clone(),
            config: call_config,
        };
        let result = match connectors::fetch(&ctx, &source, &call.query).await {
            Ok(result) => result,
            Err(error) => FetchResult {
                documents: Vec::new(),
                failures: vec![Failure {
                    stage: "fetch".into(),
                    source_id: Some(call.source_id.clone()),
                    message: error.to_string(),
                }],
            },
        };
        let result_path = format!("fetches/{index:02}/result.json");
        write_json(
            &output.join(&result_path),
            &serde_json::to_value(&result).unwrap_or(Value::Null),
        )?;
        call_records.push(json!({
            "call_index": index,
            "source_id": source.id,
            "family": source.family,
            "query": call.query,
            "reason": call.reason,
            "max_documents": call.max_documents,
            "max_pages": call.max_pages,
            "fetch_argument": call.query,
            "documents": result.documents.len(),
            "failures": result.failures.len(),
            "result_path": result_path,
            "connector_can_change_query": transform_for(&source).changes_query,
            "wire_query_observed": false,
        }));
        for failure in &result.failures {
            failures.push(failure.clone());
        }
        fetched.push(FetchedCall {
            call: call.clone(),
            result,
        });
        write_json(&output.join("calls.json"), &json!(call_records))?;
        write_json(&output.join("failures.json"), &json!(failures))?;
        write_json(&output.join("usage.json"), &usage_json(&client.usage()))?;
    }

    write_json(
        &output.join("retrieved.json"),
        &json!(fetched
            .iter()
            .flat_map(|c| c.result.documents.iter())
            .collect::<Vec<_>>()),
    )?;
    let mut classified = classify(&fetched, plan.bounds.max_documents);
    write_json(
        &output.join("duplicates.json"),
        &json!(classified.duplicates),
    )?;
    write_json(&output.join("omitted.json"), &json!(classified.omitted))?;
    let mut scores = Vec::new();
    let mut documents = Vec::new();
    let mut scoring_open = true;
    for kept in &classified.admitted {
        let scoring_document_id = scoring_id(&kept.document);
        if !scoring_open {
            classified.omitted.push(omit(
                "budget",
                "The Jev budget stopped more scores. The full document stays in this file.",
                kept.call_index,
                &kept.query,
                &kept.reason,
                &kept.document,
                None,
            ));
            continue;
        }
        let mut scored = kept.document.clone();
        scored.id = scoring_document_id.clone();
        match client.score_document(&plan.question, &scored).await {
            Ok(score) => {
                documents.push(json!({
                    "scoring_question": plan.question,
                    "search_query": kept.query,
                    "search_reason": kept.reason,
                    "call_index": kept.call_index,
                    "connector_document_id": kept.document.id,
                    "scoring_document_id": scoring_document_id,
                    "content_sha256": sha256_hex(kept.document.text.as_bytes()),
                    "document": kept.document,
                }));
                scores.push(json!({
                    "scoring_question": plan.question,
                    "search_query": kept.query,
                    "search_reason": kept.reason,
                    "call_index": kept.call_index,
                    "connector_document_id": kept.document.id,
                    "score": score,
                }));
            }
            Err(error) => {
                let message = error.to_string();
                if budget_stopped(&message) {
                    scoring_open = false;
                }
                failures.push(Failure {
                    stage: "score".into(),
                    source_id: Some(kept.document.source_id.clone()),
                    message: message.clone(),
                });
                classified.omitted.push(omit(
                    "score_error",
                    "Scoring failed. The full document stays in this file.",
                    kept.call_index,
                    &kept.query,
                    &kept.reason,
                    &kept.document,
                    None,
                ));
                if let Some(entry) = classified.omitted.last_mut() {
                    entry["error"] = json!(message);
                }
            }
        }
        write_json(&output.join("documents.json"), &json!(documents))?;
        write_json(&output.join("scores.json"), &json!(scores))?;
        write_json(&output.join("omitted.json"), &json!(classified.omitted))?;
        write_json(&output.join("failures.json"), &json!(failures))?;
        write_json(&output.join("usage.json"), &usage_json(&client.usage()))?;
    }

    let usage = client.usage();
    write_json(&output.join("calls.json"), &json!(call_records))?;
    write_json(
        &output.join("query-transforms.json"),
        &json!({
            "observation": "connectors::fetch receives the planned query. The connector can change it before HTTP. This file records the known rule. The wire query is absent from this file.",
            "original_question": plan.question,
            "original_question_planner": shared_planner(&plan.question),
            "calls": transforms,
        }),
    )?;
    write_json(&output.join("documents.json"), &json!(documents))?;
    write_json(
        &output.join("duplicates.json"),
        &json!(classified.duplicates),
    )?;
    write_json(&output.join("omitted.json"), &json!(classified.omitted))?;
    write_json(&output.join("scores.json"), &json!(scores))?;
    write_json(
        &output.join("failures.json"),
        &serde_json::to_value(&failures).map_err(|_| "Cannot encode failures.".to_owned())?,
    )?;
    write_json(
        &output.join("usage.json"),
        &serde_json::to_value(&usage).map_err(|_| "Cannot encode usage.".to_owned())?,
    )?;

    let fetched_documents: usize = fetched.iter().map(|call| call.result.documents.len()).sum();
    let status = if failures.is_empty() && scores.len() == classified.admitted.len() {
        "complete"
    } else {
        "partial"
    };
    let manifest = finish_manifest(
        output,
        plan,
        fixture,
        status,
        usage_json(&usage),
        json!({
            "calls": plan.calls.len(),
            "fetched_documents": fetched_documents,
            "admitted_documents": classified.admitted.len(),
            "scored_documents": scores.len(),
            "duplicate_groups": classified.duplicates.len(),
            "omitted_documents": classified.omitted.len(),
            "failures": failures.len(),
            "http_raw_bodies": count_raw_bodies(output)?,
        }),
    )?;
    Ok(manifest)
}

fn usage_json(usage: &stellar_raven_jev::types::Usage) -> Value {
    json!({
        "requests": usage.requests,
        "input_tokens": usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "cost_usd": usage.cost_usd,
    })
}

fn failed_manifest(
    output: &Path,
    plan: &Plan,
    fixture: bool,
    failures: &[Failure],
    usage: Option<Value>,
) -> Result<Value, String> {
    write_json(
        &output.join("failures.json"),
        &serde_json::to_value(failures).map_err(|_| "Cannot encode failures.".to_owned())?,
    )?;
    if let Some(usage) = &usage {
        write_json(&output.join("usage.json"), usage)?;
    }
    finish_manifest(
        output,
        plan,
        fixture,
        "failed",
        usage.unwrap_or(Value::Null),
        json!({
            "calls": 0,
            "fetched_documents": 0,
            "admitted_documents": 0,
            "scored_documents": 0,
            "duplicate_groups": 0,
            "omitted_documents": 0,
            "failures": failures.len(),
            "http_raw_bodies": count_raw_bodies(output)?,
        }),
    )
}

fn finish_manifest(
    output: &Path,
    plan: &Plan,
    fixture: bool,
    status: &str,
    usage: Value,
    counts: Value,
) -> Result<Value, String> {
    let mut manifest_bytes = 0u64;
    let mut manifest = Value::Null;
    for _ in 0..4 {
        let artifacts = artifact_list(output, manifest_bytes)?;
        manifest = json!({
            "schema_version": SCHEMA_VERSION,
            "status": status,
            "ok": status == "complete",
            "mode": if fixture { "fixture" } else { "live" },
            "question": plan.question,
            "scoring_question": plan.question,
            "output_dir": output.display().to_string(),
            "bounds": bounds_json(&plan.bounds),
            "http_timeout_secs": HTTP_TIMEOUT_SECS,
            "route_called": false,
            "answer_generated": false,
            "paid_jev_call": !fixture && usage.get("requests").and_then(Value::as_u64).unwrap_or(0) > 0,
            "credential_policy": "The example loads an absolute JEV_ENV_FILE when that variable is set. Otherwise it loads a local .env file. Existing variables stay in place. The output prints no credential values.",
            "counts": counts.clone(),
            "usage": usage.clone(),
            "limitations": limitations(),
            "artifacts": artifacts,
        });
        let bytes = serde_json::to_vec_pretty(&manifest)
            .map_err(|_| "Cannot encode the manifest.".to_owned())?;
        if bytes.len() as u64 == manifest_bytes {
            std::fs::write(output.join("manifest.json"), bytes)
                .map_err(|_| "Cannot write the manifest.".to_owned())?;
            return Ok(manifest);
        }
        manifest_bytes = bytes.len() as u64;
    }
    let bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|_| "Cannot encode the manifest.".to_owned())?;
    std::fs::write(output.join("manifest.json"), bytes)
        .map_err(|_| "Cannot write the manifest.".to_owned())?;
    Ok(manifest)
}

fn load_credentials() -> Result<(), String> {
    if let Some(path) = std::env::var_os("JEV_ENV_FILE") {
        let path = PathBuf::from(path);
        if !path.is_absolute() {
            return Err("JEV_ENV_FILE must be an absolute path.".into());
        }
        dotenvy::from_path(path)
            .map_err(|_| "Cannot load the explicit JEV_ENV_FILE.".to_owned())?;
    } else {
        dotenvy::dotenv().ok();
    }
    Ok(())
}

struct Args {
    fixture: bool,
    dry_run: bool,
    output_dir: Option<PathBuf>,
    plan: PathBuf,
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Args, String> {
    let mut fixture = false;
    let mut dry_run = false;
    let mut output_dir = None;
    let mut plan = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--fixture" => fixture = true,
            "--dry-run" => dry_run = true,
            "--output-dir" => {
                let path = args
                    .next()
                    .ok_or("The --output-dir flag needs a directory.".to_owned())?;
                output_dir = Some(PathBuf::from(path));
            }
            "--help" => return Err(usage()),
            other if other.starts_with('-') => {
                return Err(format!("The flag {other} is unknown. {}", usage()));
            }
            other if plan.is_none() => plan = Some(PathBuf::from(other)),
            _ => return Err(usage()),
        }
    }
    Ok(Args {
        fixture,
        dry_run,
        output_dir,
        plan: plan.ok_or_else(usage)?,
    })
}

fn usage() -> String {
    "Usage: cargo run --example query_plan -- [--fixture] [--dry-run] [--output-dir DIR] PLAN.json"
        .into()
}

fn default_output_dir() -> PathBuf {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    PathBuf::from("runs/query-plans").join(format!("{stamp}-{}", uuid::Uuid::new_v4()))
}

fn print_json(value: &Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_else(|_| "{}".into())
    );
}

fn exit_code(value: &Value) -> i32 {
    match value.get("status").and_then(Value::as_str) {
        Some("complete") | Some("dry-run") => 0,
        Some("partial") => 2,
        _ => 1,
    }
}

async fn run_args(args: Args) -> Result<Value, String> {
    let plan = load_plan(&args.plan)?;
    if args.dry_run {
        return Ok(dry_run_report(&plan));
    }
    let output = args.output_dir.unwrap_or_else(default_output_dir);
    execute(&plan, &output, args.fixture).await
}

#[tokio::main]
async fn main() {
    let code = match parse_args(std::env::args().skip(1)) {
        Ok(args) => {
            if let Err(message) = load_credentials() {
                print_json(&json!({"status": "failed", "ok": false, "error": message}));
                std::process::exit(1);
            }
            match run_args(args).await {
                Ok(value) => {
                    print_json(&value);
                    exit_code(&value)
                }
                Err(message) => {
                    print_json(&json!({"status": "failed", "ok": false, "error": message}));
                    1
                }
            }
        }
        Err(message) => {
            print_json(&json!({"status": "failed", "ok": false, "error": message}));
            1
        }
    };
    std::process::exit(code);
}

#[cfg(test)]
mod tests {
    use super::*;
    use stellar_raven_jev::types::Document;

    fn plan_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("experiments/query-plans")
            .join(name)
    }

    fn load_named(name: &str) -> Result<Plan, String> {
        load_plan(&plan_path(name))
    }

    fn document(source: &str, id: &str, text: &str) -> Document {
        Document {
            id: id.into(),
            source_id: source.into(),
            title: format!("{source} {id}"),
            url: String::new(),
            text: text.into(),
            provenance: json!({"test": true}),
            raw_artifacts: Vec::new(),
        }
    }

    fn fetched(query: &str, documents: Vec<Document>) -> FetchedCall {
        let source_id = documents
            .first()
            .map(|doc| doc.source_id.clone())
            .unwrap_or_else(|| "source".into());
        FetchedCall {
            call: Call {
                source_id,
                query: query.into(),
                reason: "Test reason.".into(),
                max_documents: 4,
                max_pages: 1,
            },
            result: FetchResult {
                documents,
                failures: Vec::new(),
            },
        }
    }

    fn read_output(dir: &Path, name: &str) -> Value {
        let bytes = std::fs::read(dir.join(name)).unwrap_or_else(|_| panic!("missing {name}"));
        serde_json::from_slice(&bytes).unwrap_or_else(|_| panic!("invalid {name}"))
    }

    #[test]
    fn sha256_matches_known_vectors() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn dry_run_keeps_the_question_and_unequal_caps() {
        let plan = load_named("valid-repeat.json").unwrap();
        let report = dry_run_report(&plan);
        assert_eq!(report["status"], "dry-run");
        assert_eq!(report["fetch_performed"], false);
        assert_eq!(report["jev_client_constructed"], false);
        assert_eq!(
            report["question"],
            "Which Stellar job listings mention Rust or Soroban work?"
        );
        assert_eq!(report["scoring_question"], report["question"]);
        assert_eq!(report["calls"][0]["query"], "Rust developer");
        assert_eq!(report["calls"][1]["query"], "Soroban engineer");
        assert_eq!(report["calls"][0]["max_documents"], 1);
        assert_eq!(report["calls"][1]["max_documents"], 3);
        assert_eq!(report["calls"][0]["max_pages"], 1);
        assert_eq!(report["calls"][1]["max_pages"], 2);
        assert_eq!(
            report["calls"][0]["transform"]["wire_query_observed"],
            false
        );
        assert_eq!(
            report["calls"][0]["transform"]["connector_can_change_query"],
            true
        );
        assert_ne!(report["calls"][0]["query"], report["question"]);
    }

    #[test]
    fn validation_rejects_bad_plan_files() {
        let cases = [
            ("invalid-unknown-source.json", "The source id is unknown"),
            ("invalid-empty-query.json", "The query is empty."),
            ("invalid-empty-reason.json", "The reason is empty."),
            (
                "invalid-url-field.json",
                "The plan field url is not allowed.",
            ),
            (
                "invalid-document-bound.json",
                "The document total exceeds the document bound.",
            ),
            ("invalid-spend.json", "The spend cap exceeds 100 USD."),
        ];
        for (name, message) in cases {
            let error = load_named(name).unwrap_err();
            assert!(
                error.contains(message),
                "{name} returned {error}, expected {message}"
            );
        }
    }

    #[test]
    fn queries_are_inert_data_and_unknown_fields_are_rejected() {
        let mut value = read_plan_value(&plan_path("valid-repeat.json")).unwrap();
        for query in [
            "https://example.org",
            "http://example.org",
            "`restoreFootprint`",
            "$(printf example)",
            "line one\nline two",
        ] {
            value["question"] = json!(query);
            value["calls"][0]["query"] = json!(query);
            assert!(parse_plan(&value).is_ok(), "{query}");
        }
        value["calls"][0]["command"] = json!("echo example");
        assert!(parse_plan(&value).is_err());
    }

    #[test]
    fn validation_rejects_page_overflow_nan_spend_and_bad_json() {
        let mut value = read_plan_value(&plan_path("valid-repeat.json")).unwrap();
        value["calls"][0]["max_pages"] = json!(3);
        value["calls"][1]["max_pages"] = json!(2);
        value["bounds"]["max_pages"] = json!(4);
        assert!(parse_plan(&value).unwrap_err().contains("page total"));
        value = read_plan_value(&plan_path("valid-repeat.json")).unwrap();
        value["bounds"]["max_spend_usd"] = json!(-1);
        assert!(parse_plan(&value).unwrap_err().contains("finite"));
        let error = read_plan_value(Path::new("experiments/query-plans/missing.json")).unwrap_err();
        assert!(error.contains("Cannot read the plan file"));
    }

    #[test]
    fn classify_keeps_duplicate_text_and_honors_the_cap() {
        let same = document("lumenloop.jobs", "lumenloop.jobs:1", "Full job text.");
        let calls = vec![
            fetched("Rust developer", vec![same.clone()]),
            fetched("Soroban engineer", vec![same.clone()]),
        ];
        let classified = classify(&calls, 6);
        assert_eq!(classified.admitted.len(), 1);
        assert_eq!(classified.omitted.len(), 1);
        assert_eq!(classified.omitted[0]["reason_code"], "duplicate");
        assert_eq!(classified.duplicates.len(), 1);
        assert_eq!(classified.duplicates[0]["match"], "source_title_url_text");
        assert_eq!(
            classified.duplicates[0]["copies"].as_array().unwrap().len(),
            2
        );
        assert_eq!(
            classified.duplicates[0]["copies"][1]["document"]["text"],
            "Full job text."
        );

        let mut other = document("lumenloop.jobs", "other", "Full job text.");
        other.title = same.title.clone();
        let classified = classify(
            &[
                fetched("one", vec![same.clone()]),
                fetched("two", vec![other]),
            ],
            6,
        );
        assert_eq!(classified.admitted.len(), 1);
        assert_eq!(classified.omitted[0]["reason_code"], "content_duplicate");

        let third = document("lumenloop.events", "event-1", "Event text.");
        let classified = classify(
            &[
                fetched("one", vec![same]),
                fetched("two", vec![third.clone()]),
            ],
            1,
        );
        assert_eq!(classified.admitted.len(), 1);
        assert_eq!(classified.omitted[0]["reason_code"], "document_limit");
        assert_eq!(classified.omitted[0]["document"]["text"], "Event text.");

        let empty = document("lumenloop.jobs", "blank", "  ");
        let classified = classify(&[fetched("blank", vec![empty])], 4);
        assert!(classified.admitted.is_empty());
        assert_eq!(classified.omitted[0]["reason_code"], "empty_text");
    }

    #[test]
    fn changed_same_id_text_title_or_url_receives_a_separate_score() {
        let first = document("lumenloop.jobs", "same", "original");
        let mut changed = first.clone();
        changed.text = "different".into();
        let mut retitled = first.clone();
        retitled.title = "different title".into();
        let mut relocated = first.clone();
        relocated.url = "https://example.org/new".into();
        let empty = document("lumenloop.jobs", "same", "");
        let result = classify(
            &[fetched(
                "q",
                vec![empty, first.clone(), changed, retitled, relocated, first],
            )],
            10,
        );
        assert_eq!(result.admitted.len(), 4);
        let ids: std::collections::BTreeSet<_> = result
            .admitted
            .iter()
            .map(|d| scoring_id(&d.document))
            .collect();
        assert_eq!(ids.len(), 4);
        assert_eq!(result.duplicates.len(), 1);
    }

    #[tokio::test]
    async fn refuses_existing_output_without_modifying_it() {
        let plan = load_named("valid-repeat.json").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let receipt = dir.path().join("receipt.txt");
        std::fs::write(&receipt, "preserve me").unwrap();
        assert!(execute(&plan, dir.path(), true)
            .await
            .unwrap_err()
            .contains("already exists"));
        assert_eq!(std::fs::read_to_string(receipt).unwrap(), "preserve me");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn budget_stop_detects_the_client_message() {
        assert!(budget_stopped(
            "Jev budget cannot cover another complete attempt"
        ));
        assert!(!budget_stopped("The document has no text to score"));
    }

    #[tokio::test]
    async fn fixture_repeat_scores_the_original_question_once() {
        let plan = load_named("valid-repeat.json").unwrap();
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("run");
        let manifest = execute(&plan, &dir, true).await.unwrap();
        assert_eq!(manifest["status"], "complete");
        assert_eq!(manifest["mode"], "fixture");
        assert_eq!(manifest["paid_jev_call"], false);
        assert_eq!(manifest["question"], plan.question);
        assert_eq!(manifest["route_called"], false);
        assert_eq!(manifest["counts"]["calls"], 2);
        assert_eq!(manifest["counts"]["fetched_documents"], 2);
        assert_eq!(manifest["counts"]["scored_documents"], 1);
        assert_eq!(manifest["counts"]["http_raw_bodies"], 0);
        assert_eq!(manifest["usage"]["requests"], 0);
        assert_eq!(manifest["usage"]["cost_usd"], 0.0);

        let calls = read_output(dir.as_path(), "calls.json");
        assert_eq!(calls[0]["fetch_argument"], "Rust developer");
        assert_eq!(calls[1]["fetch_argument"], "Soroban engineer");
        assert_eq!(calls[0]["max_documents"], 1);
        assert_eq!(calls[1]["max_documents"], 3);
        assert_eq!(calls[0]["max_pages"], 1);
        assert_eq!(calls[1]["max_pages"], 2);
        assert_ne!(calls[0]["fetch_argument"], plan.question);

        let first = read_output(dir.as_path(), "fetches/00/result.json");
        let second = read_output(dir.as_path(), "fetches/01/result.json");
        let text = first["documents"][0]["text"].as_str().unwrap();
        assert!(!text.is_empty());
        assert_eq!(second["documents"][0]["text"], text);

        let documents = read_output(dir.as_path(), "documents.json");
        assert_eq!(documents.as_array().unwrap().len(), 1);
        assert_eq!(documents[0]["scoring_question"], plan.question);
        assert_eq!(documents[0]["document"]["text"], text);

        let scores = read_output(dir.as_path(), "scores.json");
        assert_eq!(scores.as_array().unwrap().len(), 1);
        assert_eq!(scores[0]["scoring_question"], plan.question);
        assert_ne!(scores[0]["search_query"], plan.question);
        assert_eq!(scores[0]["score"]["probability"], 0.8);

        let duplicates = read_output(dir.as_path(), "duplicates.json");
        assert_eq!(duplicates[0]["copies"].as_array().unwrap().len(), 2);
        assert_eq!(duplicates[0]["copies"][0]["document"]["text"], text);
        assert_eq!(duplicates[0]["copies"][1]["document"]["text"], text);
        let omitted = read_output(dir.as_path(), "omitted.json");
        assert_eq!(omitted[0]["reason_code"], "duplicate");
        assert_eq!(omitted[0]["document"]["text"], text);

        let transforms = read_output(dir.as_path(), "query-transforms.json");
        assert_eq!(transforms["original_question"], plan.question);
        assert_eq!(transforms["calls"][0]["wire_query_observed"], false);
        assert_eq!(transforms["calls"][0]["connector_can_change_query"], true);
        assert!(transforms["calls"][0]["summary"]
            .as_str()
            .unwrap()
            .contains("Jobs search"));
        assert_eq!(
            transforms["calls"][0]["shared_planner"]["question"],
            "Rust developer"
        );

        let policy = dir.as_path().join("jev").join("accounting-policy.json");
        let policy: Value = serde_json::from_slice(&std::fs::read(policy).unwrap()).unwrap();
        assert_eq!(policy["backend"], "fixture");
        let fixture_audits = std::fs::read_dir(dir.as_path().join("jev"))
            .unwrap()
            .filter(|entry| {
                entry
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("fixture-document-")
            })
            .count();
        assert_eq!(fixture_audits, 1);
        assert!(dir.as_path().join("raw").is_dir());
    }

    #[tokio::test]
    async fn fixture_two_sources_record_different_query_rules() {
        let plan = load_named("valid-two-sources.json").unwrap();
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("run");
        let manifest = execute(&plan, &dir, true).await.unwrap();
        assert_eq!(manifest["status"], "complete");
        assert_eq!(manifest["counts"]["scored_documents"], 2);
        assert_eq!(manifest["counts"]["duplicate_groups"], 0);
        assert_eq!(manifest["usage"]["requests"], 0);
        let scores = read_output(dir.as_path(), "scores.json");
        assert_eq!(scores[0]["scoring_question"], plan.question);
        assert_eq!(scores[1]["scoring_question"], plan.question);
        assert_ne!(scores[0]["search_query"], scores[1]["search_query"]);
        let transforms = read_output(dir.as_path(), "query-transforms.json");
        assert_eq!(transforms["calls"][0]["family"], "algolia");
        assert_eq!(transforms["calls"][1]["source_id"], "stellarlight.skills");
        assert!(transforms["calls"][0]["summary"]
            .as_str()
            .unwrap()
            .contains("keyword"));
        assert!(transforms["calls"][1]["summary"]
            .as_str()
            .unwrap()
            .contains("fixed catalog"));
        assert_eq!(transforms["calls"][0]["wire_query_observed"], false);
        assert_eq!(transforms["calls"][1]["wire_query_observed"], false);
        let docs = read_output(dir.as_path(), "documents.json");
        assert!(!docs[0]["document"]["text"].as_str().unwrap().is_empty());
        assert!(!docs[1]["document"]["text"].as_str().unwrap().is_empty());
        let audits = std::fs::read_dir(dir.as_path().join("jev"))
            .unwrap()
            .filter(|entry| {
                entry
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("fixture-document-")
            })
            .count();
        assert_eq!(audits, 2);
    }

    #[tokio::test]
    async fn live_zero_spend_stops_before_a_directory_or_request() {
        let plan = load_named("valid-repeat.json").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("live-out");
        let error = execute(&plan, &output, false).await.unwrap_err();
        assert!(error.contains("spend cap above zero"));
        assert!(!output.exists());
    }

    #[test]
    fn args_accept_fixture_dry_run_and_output_dir() {
        let args = parse_args([
            "--dry-run".into(),
            "--fixture".into(),
            "--output-dir".into(),
            "runs/example".into(),
            "plan.json".into(),
        ])
        .unwrap();
        assert!(args.dry_run);
        assert!(args.fixture);
        assert_eq!(args.output_dir.unwrap(), PathBuf::from("runs/example"));
        assert_eq!(args.plan, PathBuf::from("plan.json"));
        assert!(parse_args(["--url".into(), "plan.json".into()]).is_err());
    }
}
