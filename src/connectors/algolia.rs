//! Read-only Algolia retrieval of the stellar.org site indexes.
use super::published;
use crate::types::*;
use anyhow::{bail, Result};
use reqwest::{Method, Url};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

const SITE: [&str; 18] = [
    "dev_pages",
    "dev_pages_asc",
    "dev_pages_desc",
    "dev_pages_es",
    "dev_pages_es_asc",
    "dev_pages_es_desc",
    "pages",
    "pages_asc",
    "pages_desc",
    "pages_es",
    "pages_es_asc",
    "pages_es_desc",
    "staging_pages",
    "staging_pages_asc",
    "staging_pages_desc",
    "staging_pages_es",
    "staging_pages_es_asc",
    "staging_pages_es_desc",
];

#[derive(Clone, Copy)]
struct Index {
    name: &'static str,
}
impl Index {
    fn id(self) -> String {
        format!("algolia:site:{}", self.name)
    }
    fn production(self) -> bool {
        !self.name.starts_with("dev_") && !self.name.starts_with("staging_")
    }
    fn spanish(self) -> bool {
        self.name.contains("_es")
    }
    fn replica(self) -> bool {
        self.name.ends_with("_asc") || self.name.ends_with("_desc")
    }
}

fn indices() -> impl Iterator<Item = Index> {
    SITE.into_iter().map(|name| Index { name })
}

pub fn sources() -> Vec<Source> {
    indices().filter(|index| !index.replica()).map(|index| {
        let content = "Official Stellar site pages, announcements, events, partners, audits, and page sections.";
        let locale = if index.spanish() { "Spanish." } else { "English." };
        let environment = if index.production() { "Production index." } else {
            "Development or staging index. Content can be stale, unpublished, or different from production."
        };
        let replica = if index.replica() { "Replica of another index. Results are not independent evidence." } else { "Primary index." };
        Source { id: index.id(), name: format!("Algolia SITE / {}", index.name),
            description: format!("{content} {locale} {environment} {replica}"), family: "algolia".into() }
    }).collect()
}
fn failure(result: &mut FetchResult, source: &Source, stage: &str, message: impl Into<String>) {
    result.failures.push(Failure {
        stage: stage.into(),
        source_id: Some(source.id.clone()),
        message: message.into(),
        cause: None,
    });
}
fn endpoint(app: &str, index: Option<&str>) -> Result<Url> {
    let mut url = Url::parse(&format!("https://{app}-dsn.algolia.net/1/indexes"))?;
    if let Some(index) = index {
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("Invalid Algolia endpoint"))?
            .push(index)
            .push("query");
    }
    Ok(url)
}
fn search_params(question: &str, page: usize, size: usize) -> Value {
    json!({"query":question,"page":page,"hitsPerPage":size,"analytics":false,"clickAnalytics":false,
        "distinct":false,"attributesToRetrieve":["*"],"attributesToHighlight":[],"attributesToSnippet":[],
        "removeWordsIfNoResults":"none"})
}
fn local_site_keyword_variant(question: &str) -> Option<String> {
    let stop = [
        "what",
        "which",
        "who",
        "where",
        "when",
        "why",
        "how",
        "do",
        "does",
        "did",
        "can",
        "could",
        "would",
        "should",
        "is",
        "are",
        "was",
        "were",
        "will",
        "have",
        "has",
        "had",
        "the",
        "a",
        "an",
        "and",
        "or",
        "for",
        "of",
        "on",
        "in",
        "to",
        "from",
        "with",
        "about",
        "me",
        "tell",
        "please",
        "explain",
        "describe",
        "show",
        "list",
        "give",
        "find",
        "information",
        "using",
        "i",
        "we",
        "you",
        "this",
        "that",
        "these",
        "those",
    ];
    let words: Vec<_> = question
        .split_whitespace()
        .map(|s| {
            s.trim_matches(|c: char| !c.is_alphanumeric() && !matches!(c, '-' | '_' | '.' | ':'))
        })
        .filter(|s| !s.is_empty() && !stop.contains(&s.to_ascii_lowercase().as_str()))
        .collect();
    let entities: Vec<_> = words
        .iter()
        .copied()
        .filter(|word| {
            word.chars().any(|c| c.is_uppercase() || c.is_ascii_digit())
                || word.contains(['-', '_'])
        })
        .collect();
    // Preserve every detected entity. Never invent terms or issue an empty query.
    let selected = if entities.is_empty() { words } else { entities };
    let variant = selected.join(" ");
    if variant.is_empty() || variant == question {
        None
    } else {
        Some(variant)
    }
}

fn site_keyword_variant(question: &str) -> Option<String> {
    let local = local_site_keyword_variant(question)?;
    let local_tokens: Vec<_> = local.split_whitespace().collect();
    let entities: Vec<_> = local_tokens
        .iter()
        .copied()
        .filter(|word| {
            word.chars().any(|c| c.is_uppercase() || c.is_ascii_digit())
                || word.contains(['-', '_'])
        })
        .collect();
    let plan = crate::query::plan(question);
    plan.keyword()
        .into_iter()
        .find(|variant| {
            let tokens: Vec<_> = variant.text.split_whitespace().collect();
            variant.kind == crate::query::VariantKind::Keywords
                && !tokens.is_empty()
                && variant.text != question.trim()
                && tokens.len() <= local_tokens.len()
                && entities.iter().all(|entity| tokens.contains(entity))
        })
        .map(|variant| variant.text.clone())
        .or(Some(local))
}

fn planned_query(index: Index, plan: &crate::query::QueryPlan) -> (String, &'static str) {
    if plan.non_english && !index.spanish() {
        let entities = plan
            .entity()
            .iter()
            .map(|variant| variant.text.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        if !entities.is_empty() {
            return (entities, "entity");
        }
    }
    let text = plan.keyword_text();
    let kind = if text == plan.question {
        "natural"
    } else {
        "keywords"
    };
    (text, kind)
}

fn canonical(index: Index, hit: &Value) -> Option<Url> {
    let raw = hit
        .get("url_without_anchor")
        .and_then(Value::as_str)
        .or_else(|| hit.get("url").and_then(Value::as_str))?;
    // Reject schemes, credentials, ports, and off-site URLs before any original fetch.
    let mut url = if raw.starts_with('/') && !raw.starts_with("//") {
        Url::parse("https://stellar.org").ok()?.join(raw).ok()?
    } else {
        Url::parse(raw).ok()?
    };
    if url.scheme() != "https"
        || url.host_str() != Some("stellar.org")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
    {
        return None;
    }
    if url.path().contains('\\')
        || url.path().to_lowercase().contains("%2f")
        || url.path().to_lowercase().contains("%5c")
    {
        return None;
    }
    url.set_query(None);
    url.set_fragment(None);
    if index.spanish() && url.path() != "/es" && !url.path().starts_with("/es/") {
        url.set_path(&format!("/es{}", url.path()));
    }
    Some(url)
}
fn title(hit: &Value) -> String {
    hit.get("title")
        .and_then(Value::as_str)
        .or_else(|| hit.pointer("/parent/title").and_then(Value::as_str))
        .or_else(|| hit.pointer("/hierarchy/lvl1").and_then(Value::as_str))
        .or_else(|| hit.pointer("/hierarchy/lvl0").and_then(Value::as_str))
        .unwrap_or("Untitled Algolia record")
        .to_owned()
}
fn record_text(hit: &Value) -> String {
    // Keep structured body fields intact instead of coercing them to a string placeholder.
    [
        "content",
        "body",
        "items",
        "description",
        "title",
        "hierarchy",
    ]
    .iter()
    .filter_map(|key| {
        hit.get(key).filter(|v| !v.is_null()).map(|v| {
            if let Some(s) = v.as_str() {
                s.to_owned()
            } else {
                v.to_string()
            }
        })
    })
    .collect::<Vec<_>>()
    .join("\n\n")
}
fn hit_provenance(hit: &Value) -> Value {
    json!({"object_id":hit.get("objectID"),"indexed_url":hit.get("url"),"anchor":hit.get("anchor"),
        "record_type":hit.get("type").or_else(||hit.get("_type")),"language":hit.get("language"),
        "date":hit.get("date"),"hierarchy":hit.get("hierarchy"),"parent":hit.get("parent")})
}
fn timestamp_seconds(s: &str) -> Option<u64> {
    // Parse the UTC format returned by index listing without adding a shared dependency.
    let b = s.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || !s.ends_with('Z') {
        return None;
    }
    let n = |a: usize, z: usize| s.get(a..z)?.parse::<u64>().ok();
    let (y, m, d, h, mi, se) = (
        n(0, 4)?,
        n(5, 7)?,
        n(8, 10)?,
        n(11, 13)?,
        n(14, 16)?,
        n(17, 19)?,
    );
    if !(1970..=3000).contains(&y) || !(1..=12).contains(&m) || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let leap = |y: u64| y.is_multiple_of(4) && (!y.is_multiple_of(100) || y.is_multiple_of(400));
    let months = [
        31,
        if leap(y) { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if d == 0 || d > months[(m - 1) as usize] {
        return None;
    }
    let days: u64 = (1970..y)
        .map(|year| if leap(year) { 366 } else { 365 })
        .sum::<u64>()
        + months[..(m - 1) as usize].iter().sum::<u64>()
        + d
        - 1;
    Some(days * 86400 + h * 3600 + mi * 60 + se)
}

/// The published page of a production record. Development and staging records have none.
async fn original(ctx: &FetchContext, source: &Source, index: Index, url: &Url) -> published::Read {
    if !index.production() {
        return published::Read {
            text: None,
            meta: json!({"kind":"index_record","full_original":false,
                "limitation":"No published original fetch for development or staging records. Published content can differ."}),
            artifacts: vec![],
            failures: vec![],
        };
    }
    published::read(ctx, source, url, false).await
}

/// The search host of an Algolia source: its application's endpoint, when configured.
pub fn search_host(source: &Source) -> Option<String> {
    indices().find(|index| index.id() == source.id)?;
    let app = std::env::var("ALGOLIA_APPLICATION_ID_SITE")
        .ok()
        .filter(|s| !s.is_empty())?;
    endpoint(&app, None).ok()?.host_str().map(str::to_owned)
}

pub async fn fetch(ctx: &FetchContext, source: &Source, question: &str) -> Result<FetchResult> {
    let index = indices()
        .find(|index| index.id() == source.id)
        .ok_or_else(|| anyhow::anyhow!("Unknown Algolia source"))?;
    let mut result = FetchResult::default();
    if ctx.config.max_pages == 0 || ctx.config.max_documents == 0 {
        failure(
            &mut result,
            source,
            "limit",
            "The page or document limit prevents retrieval.",
        );
        return Ok(result);
    }
    if ctx.config.fixture {
        result.documents.push(Document { id:format!("{}:fixture",source.id), source_id:source.id.clone(),
            title:format!("Fixture: {}",index.name),url:"https://stellar.org/enterprise-fund".into(),
            text:"Offline Algolia fixture. This is synthetic content, not a live source response.".into(),
            provenance:json!({"fixture":true,"application":"SITE","index":index.name,"content_kind":"fixture","full_original":false}),raw_artifacts:vec![] });
        return Ok(result);
    }
    let query_plan = crate::query::plan(question);
    for omission in &query_plan.omissions {
        failure(
            &mut result,
            source,
            "query_plan",
            format!(
                "Shared planner omission: {}",
                json!({"stage":omission.stage,"text":omission.text,"reason":omission.reason})
            ),
        );
    }
    let app = std::env::var("ALGOLIA_APPLICATION_ID_SITE")
        .ok()
        .filter(|s| !s.is_empty());
    let key = std::env::var("ALGOLIA_API_KEY_SITE")
        .ok()
        .filter(|s| !s.is_empty());
    let (app, key) = match (app, key) {
        (Some(app), Some(key)) => (app, key),
        _ => {
            failure(
                &mut result,
                source,
                "authentication",
                "Missing Algolia SITE read credentials.",
            );
            return Ok(result);
        }
    };
    if !app.bytes().all(|b| b.is_ascii_alphanumeric()) || app.len() > 32 {
        bail!("Invalid Algolia application identifier");
    }
    let headers = vec![
        ("X-Algolia-Application-Id".into(), app.clone()),
        ("X-Algolia-API-Key".into(), key),
    ];
    if !index.production() {
        failure(
            &mut result,
            source,
            "source_limit",
            "Development or staging content does not establish current production facts.",
        );
    }
    let url = endpoint(&app, Some(index.name))?;
    let size = ctx.config.max_documents.min(100);
    let mut page = 0;
    let (mut active_query, mut query_kind) = planned_query(index, &query_plan);
    let mut keyword_retry = false;
    let mut variants = Vec::<Value>::new();
    let mut seen = HashSet::new();
    let mut pages_by_url: HashMap<String, usize> = HashMap::new();
    // The index inventory only annotates freshness, so it runs alongside the search. Every
    // index of one application shares the same inventory response within a run.
    let mut pending: Vec<(usize, Url)> = Vec::new();
    let search = async {
        for request_number in 0..ctx.config.max_pages {
            let params = search_params(&active_query, page, size);
            variants.push(json!({"query":active_query,"page":page,"matching_mode":"none",
                "kind":query_kind,"variant":if keyword_retry {"site_shared_keyword_or_local_entity"} else {"shared_initial"}}));
            let response = match ctx
                .http
                .request(Method::POST, url.as_str(), headers.clone(), Some(params))
                .await
            {
                Ok(response) => response,
                Err(_) => {
                    failure(
                        &mut result,
                        source,
                        "search",
                        "The search request failed. Partial results remain available.",
                    );
                    break;
                }
            };
            if response.status != 200 {
                failure(
                    &mut result,
                    source,
                    "search",
                    format!(
                        "Search returned HTTP {}. Artifact: {}",
                        response.status, response.artifact
                    ),
                );
                break;
            }
            let body = match response.json() {
                Ok(body) => body,
                Err(_) => {
                    failure(
                        &mut result,
                        source,
                        "search",
                        format!(
                            "Search returned malformed JSON. Artifact: {}",
                            response.artifact
                        ),
                    );
                    break;
                }
            };
            if body.get("message").is_some() {
                failure(
                    &mut result,
                    source,
                    "search_notice",
                    format!(
                        "The search response contains an API warning. Artifact: {}",
                        response.artifact
                    ),
                );
            }
            let hits = match body.get("hits").and_then(Value::as_array) {
                Some(hits) => hits,
                None => {
                    failure(
                        &mut result,
                        source,
                        "search",
                        format!("Search has no hits array. Artifact: {}", response.artifact),
                    );
                    break;
                }
            };
            let total = body.get("nbHits").and_then(Value::as_u64);
            if total.is_none() || body.get("nbPages").and_then(Value::as_u64).is_none() {
                failure(
                    &mut result,
                    source,
                    "search_notice",
                    "Search count metadata is missing. Completeness remains unknown.",
                );
            }
            if total.is_some_and(|n| n > 1000) {
                failure(
                    &mut result,
                    source,
                    "limit",
                    "Search matches exceed the verified 1,000-record pagination limit.",
                );
            }
            if body.get("exhaustiveNbHits") == Some(&Value::Bool(false))
                || body.pointer("/exhaustive/nbHits") == Some(&Value::Bool(false))
            {
                failure(
                    &mut result,
                    source,
                    "limit",
                    "Algolia reports an approximate hit count.",
                );
            }
            for hit in hits {
                let Some(id) = hit.get("objectID").and_then(Value::as_str) else {
                    failure(
                        &mut result,
                        source,
                        "record",
                        "An indexed record has no objectID.",
                    );
                    continue;
                };
                if !seen.insert(id.to_owned()) {
                    continue;
                }
                let canonical = canonical(index, hit);
                let group = canonical
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| format!("record:{id}"));
                if let Some(&position) = pages_by_url.get(&group) {
                    let doc = &mut result.documents[position];
                    doc.provenance["records"]
                        .as_array_mut()
                        .unwrap()
                        .push(hit_provenance(hit));
                    if doc.provenance["original"]["full_original"] != true {
                        doc.text.push_str("\n\n");
                        doc.text.push_str(&record_text(hit));
                    }
                    if !doc.raw_artifacts.contains(&response.artifact) {
                        doc.raw_artifacts.push(response.artifact.clone());
                    }
                    continue;
                }
                if result.documents.len() >= ctx.config.max_documents {
                    failure(&mut result,source,"limit","The document limit omitted indexed candidates. Full search responses retain them.");
                    break;
                }
                let artifacts = vec![response.artifact.clone()];
                // Originals are read together after the search pages; until then the record stands in.
                let original_meta = if let Some(url) = &canonical {
                    pending.push((result.documents.len(), url.clone()));
                    json!({"kind":"index_record","full_original":false,"limitation":"The original is read after the search."})
                } else {
                    failure(&mut result,source,"original","A record has no allowed canonical URL. The connector retained its indexed content.");
                    json!({"kind":"index_record","full_original":false,"limitation":"No allowed canonical URL."})
                };
                let doc = Document {
                    id: format!("{}:{id}", source.id),
                    source_id: source.id.clone(),
                    title: title(hit),
                    url: canonical.map(|url| url.to_string()).unwrap_or_else(|| {
                        hit.get("url")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_owned()
                    }),
                    text: record_text(hit),
                    provenance: json!({"application":"SITE","index":index.name,"replica":index.replica(),"production":index.production(),
                        "query":question,"query_variant":{"kind":query_kind,"text":active_query},"query_used":active_query,"query_variants":variants,"search_page":page,"records":[hit_provenance(hit)],"content_scope":original_meta.get("content_scope").cloned().unwrap_or(json!("indexed_sections_or_metadata")),
                        "extraction_limitations":original_meta.get("extraction_limitations").cloned().unwrap_or(json!(["Only indexed sections or metadata are available."])),"original":original_meta,
                        "index_metadata":null,"index_age_seconds":null,"index_inventory_verified_at_unix":null,
                        "limitations":["Index timestamps do not prove document freshness or corpus completeness.",
                            "Search can omit documents. Replicas do not supply independent corroboration."]}),
                    raw_artifacts: artifacts,
                };
                pages_by_url.insert(group, result.documents.len());
                result.documents.push(doc);
            }
            if hits.is_empty()
                && page == 0
                && !keyword_retry
                && total == Some(0)
                && body.get("message").is_none()
                && request_number + 1 < ctx.config.max_pages
            {
                if let Some(variant) =
                    site_keyword_variant(question).filter(|variant| variant != &active_query)
                {
                    let shared = query_plan.keyword().iter().any(|candidate| {
                        candidate.kind == crate::query::VariantKind::Keywords
                            && candidate.text == variant
                    });
                    failure(&mut result,source,"query_variant",format!(
                        "The SITE search returned no hits. One keyword retry retains detected entities. Changes: {}",
                        json!({"original":question,"variant":variant,
                            "method":if shared {"shared_keyword"} else {"local_entity_fallback"},
                            "empty_query":false,"selection":"First nonempty shared keyword preserves entities and does not add AND terms. Otherwise retain the local fallback."})));
                    active_query = variant;
                    query_kind = if shared {
                        "keywords"
                    } else {
                        "local_entity_fallback"
                    };
                    keyword_retry = true;
                    continue;
                }
            }
            let pages = body.get("nbPages").and_then(Value::as_u64).unwrap_or(0) as usize;
            if page + 1 >= pages {
                break;
            }
            if request_number + 1 >= ctx.config.max_pages
                || result.documents.len() >= ctx.config.max_documents
                || (page + 1) * size >= 1000
            {
                failure(&mut result,source,"limit","The page, document, or Algolia limit stopped retrieval before all matching records.");
                break;
            }
            page += 1;
        }
        // Read the originals together, bounded, and apply them in hit order.
        use futures::StreamExt;
        let originals: Vec<_> =
            futures::stream::iter(std::mem::take(&mut pending))
                .map(|(position, url)| async move {
                    (position, original(ctx, source, index, &url).await)
                })
                .buffered(published::READS)
                .collect()
                .await;
        for (
            position,
            published::Read {
                text,
                meta,
                artifacts,
                mut failures,
            },
        ) in originals
        {
            result.failures.append(&mut failures);
            let doc = &mut result.documents[position];
            if let Some(text) = text {
                doc.text = text;
            }
            doc.provenance["content_scope"] = meta
                .get("content_scope")
                .cloned()
                .unwrap_or(json!("indexed_sections_or_metadata"));
            doc.provenance["extraction_limitations"] = meta
                .get("extraction_limitations")
                .cloned()
                .unwrap_or(json!(["Only indexed sections or metadata are available."]));
            doc.provenance["original"] = meta;
            doc.raw_artifacts.extend(artifacts);
        }
    };
    let inventory_url = endpoint(&app, None)?;
    let (inventory_response, ()) = tokio::join!(
        ctx.http.get_shared(inventory_url.as_str(), headers.clone()),
        search
    );
    let mut inventory = Value::Null;
    let mut inventory_artifact = None;
    match inventory_response {
        Ok(response) => {
            inventory_artifact = Some(response.artifact.clone());
            if response.status == 200 {
                if let Ok(value) = response.json() {
                    inventory = value
                        .get("items")
                        .and_then(Value::as_array)
                        .and_then(|items| {
                            items.iter().find(|item| {
                                item.get("name").and_then(Value::as_str) == Some(index.name)
                            })
                        })
                        .cloned()
                        .unwrap_or(Value::Null);
                }
            }
            if inventory.is_null() {
                failure(
                    &mut result,
                    source,
                    "freshness",
                    format!(
                        "The live inventory did not verify this index. Artifact: {}",
                        response.artifact
                    ),
                );
            }
        }
        Err(_) => failure(
            &mut result,
            source,
            "freshness",
            "The live index inventory request failed. Index freshness remains unknown.",
        ),
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let age = inventory
        .get("updatedAt")
        .and_then(Value::as_str)
        .and_then(timestamp_seconds)
        .map(|t| now.saturating_sub(t));
    if age.is_some_and(|age| age > 48 * 3600) {
        failure(
            &mut result,
            source,
            "freshness",
            "The index update is older than 48 hours. Records can be stale.",
        );
    }
    for doc in &mut result.documents {
        doc.provenance["index_metadata"] = inventory.clone();
        doc.provenance["index_age_seconds"] = json!(age);
        doc.provenance["index_inventory_verified_at_unix"] = json!(now);
        if let Some(artifact) = &inventory_artifact {
            doc.raw_artifacts
                .insert(1.min(doc.raw_artifacts.len()), artifact.clone());
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn catalog_avoids_replica_fanout_and_inventory_covers_every_index() {
        let sources = sources();
        assert_eq!(indices().count(), 18);
        assert_eq!(sources.len(), 6);
        assert_eq!(
            sources.iter().map(|s| &s.id).collect::<HashSet<_>>().len(),
            6
        );
    }
    #[test]
    fn spanish_originals_use_the_verified_locale_prefix() {
        let index = Index { name: "pages_es" };
        assert_eq!(
            canonical(index, &json!({"url":"/fondo-empresarial#x"}))
                .unwrap()
                .as_str(),
            "https://stellar.org/es/fondo-empresarial"
        );
        assert_eq!(
            canonical(index, &json!({"url":"/es/fundacion"}))
                .unwrap()
                .as_str(),
            "https://stellar.org/es/fundacion"
        );
    }
    #[test]
    fn originals_never_use_an_untrusted_host_or_scheme() {
        let index = Index { name: "pages" };
        for url in [
            "https://evil.test/page",
            "//evil.test/page",
            "http://stellar.org/a",
            "https://user@stellar.org/a",
            "https://stellar.org:8443/a",
            "file:///tmp/a",
            "https://stellar.org/%2f%2fevil.test",
        ] {
            assert!(canonical(index, &json!({"url":url})).is_none(), "{url}");
        }
    }
    #[test]
    fn source_text_preserves_structured_section_content() {
        let text = record_text(
            &json!({"body":[{"children":[{"text":"Full nested body"}]}],"content":null}),
        );
        assert!(text.contains("Full nested body"));
        assert!(!text.contains("[object Object]"));
    }
    #[test]
    fn timestamp_parser_handles_dates_and_rejects_invalid_input() {
        assert_eq!(timestamp_seconds("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            timestamp_seconds("2026-09-21T12:03:44.742Z"),
            Some(1789992224)
        );
        assert_eq!(timestamp_seconds("2025-02-29T00:00:00Z"), None);
    }
    #[test]
    fn searches_always_disable_analytics_and_retrieve_all_attributes() {
        let p = search_params("q", 1, 20);
        assert_eq!(p["analytics"], false);
        assert_eq!(p["clickAnalytics"], false);
        assert_eq!(p["distinct"], false);
        assert_eq!(p["attributesToRetrieve"], json!(["*"]));
    }
    #[test]
    fn site_retry_preserves_entities_without_an_empty_query() {
        let question = "What companies received funding from the Stellar Widget Fund?";
        let plan = crate::query::plan(question);
        assert_ne!(plan.keyword()[0].text, "Stellar Widget Fund");
        assert_eq!(
            site_keyword_variant("Tell me about ACMEUSD adoption on Stellar"),
            local_site_keyword_variant("Tell me about ACMEUSD adoption on Stellar")
        );
        assert_eq!(
            site_keyword_variant("What companies received funding from the Stellar Widget Fund?"),
            Some("Stellar Widget Fund".into())
        );
        assert_eq!(
            site_keyword_variant("Tell me about ACMEUSD adoption on Stellar"),
            Some("ACMEUSD Stellar".into())
        );
        assert_eq!(site_keyword_variant("What is the?"), None);
        assert_eq!(site_keyword_variant("ACMEUSD Stellar"), None);
    }
    #[test]
    fn site_retry_uses_the_first_shared_keyword_within_one_retry() {
        let question = "Find hardware wallet projects and sources on account recovery when the original device is lost.";
        assert_eq!(
            site_keyword_variant(question),
            Some("hardware wallet projects".into())
        );
        assert_eq!(
            site_keyword_variant(question).unwrap(),
            crate::query::plan(question).keyword()[0].text
        );
    }
    #[test]
    fn initial_query_uses_shared_content_words_and_respects_index_language() {
        let plan = crate::query::plan("How do I rotate expired Widgetd signing keys?");
        assert_eq!(
            planned_query(Index { name: "pages" }, &plan),
            ("rotate expired Widgetd signing keys".into(), "keywords")
        );
        let spanish = crate::query::plan(
            "¿Dónde encuentro documentación sobre límites y firmas de transacciones de Widgetd?",
        );
        assert_eq!(
            planned_query(Index { name: "pages_es" }, &spanish).0,
            spanish.keyword_text()
        );
        assert_eq!(
            planned_query(Index { name: "pages" }, &spanish),
            ("Widgetd".into(), "entity")
        );
    }
    #[tokio::test]
    async fn fixture_runs_without_credentials_or_network() {
        let config = RunConfig {
            fixture: true,
            ..Default::default()
        };
        let dir = std::env::temp_dir().join(format!("algolia-fixture-{}", uuid::Uuid::new_v4()));
        let http = crate::http::HttpRecorder::new(&dir, &config).unwrap();
        let ctx = FetchContext {
            http,
            config,
            deadline: None,
        };
        for source in sources() {
            let a = fetch(&ctx, &source, "question").await.unwrap();
            let b = fetch(&ctx, &source, "question").await.unwrap();
            assert_eq!(a.documents.len(), 1);
            assert_eq!(a.documents[0].text, b.documents[0].text);
            assert_eq!(a.documents[0].provenance["fixture"], true);
            assert!(a.documents[0].raw_artifacts.is_empty());
        }
        let _ = std::fs::remove_dir_all(dir);
    }
}
