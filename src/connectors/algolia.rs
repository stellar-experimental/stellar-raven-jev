//! Read-only Algolia retrieval.
use crate::extract::{html_article_text, markdown_for_scoring, unresolved_markdown_component};
use crate::types::*;
use anyhow::{bail, Result};
use reqwest::{Method, Url};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

const DOCS: [&str; 2] = ["crawler_Stellar Docs - Docusaurus", "docs_replica_agent"];
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
    scope: &'static str,
    name: &'static str,
}
impl Index {
    fn id(self) -> String {
        if self.name == DOCS[0] {
            "algolia:docs:primary".into()
        } else {
            format!("algolia:{}:{}", self.scope.to_lowercase(), self.name)
        }
    }
    fn production(self) -> bool {
        !self.name.starts_with("dev_") && !self.name.starts_with("staging_")
    }
    fn spanish(self) -> bool {
        self.scope == "SITE" && self.name.contains("_es")
    }
    fn replica(self) -> bool {
        self.name == DOCS[1] || self.name.ends_with("_asc") || self.name.ends_with("_desc")
    }
}
/// Original pages read at once by one connector.
const ORIGINAL_READS: usize = 6;

fn indices() -> impl Iterator<Item = Index> {
    DOCS.into_iter()
        .map(|name| Index {
            scope: "DOCS",
            name,
        })
        .chain(SITE.into_iter().map(|name| Index {
            scope: "SITE",
            name,
        }))
}

pub fn sources() -> Vec<Source> {
    indices().filter(|index| !index.replica()).map(|index| {
        let content = if index.scope == "DOCS" {
            "Official developer documentation and protocol meeting notes. Records contain headings or sections."
        } else {
            "Official Stellar site pages, announcements, events, partners, audits, and page sections."
        };
        let locale = if index.spanish() { "Spanish." } else { "English." };
        let environment = if index.production() { "Production index." } else {
            "Development or staging index. Content can be stale, unpublished, or different from production."
        };
        let replica = if index.replica() { "Replica of another index. Results are not independent evidence." } else { "Primary index." };
        Source { id: index.id(), name: format!("Algolia {} / {}", index.scope, index.name),
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
fn search_params(question: &str, page: usize, size: usize, relaxed: bool) -> Value {
    json!({"query":question,"page":page,"hitsPerPage":size,"analytics":false,"clickAnalytics":false,
        "distinct":false,"attributesToRetrieve":["*"],"attributesToHighlight":[],"attributesToSnippet":[],
        "removeWordsIfNoResults":if relaxed { "allOptional" } else { "none" }})
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

fn docs_facet_queries(
    plan: &crate::query::QueryPlan,
    max_pages: usize,
    max_documents: usize,
) -> Vec<String> {
    if max_pages < 2 || max_documents < 2 {
        return Vec::new();
    }
    let mut seen = HashSet::new();
    let queries: Vec<_> = plan
        .keyword()
        .into_iter()
        .filter(|variant| {
            variant.kind == crate::query::VariantKind::Keywords
                && !variant.text.trim().is_empty()
                && variant.facet.is_some_and(|facet| seen.insert(facet))
        })
        .map(|variant| variant.text.clone())
        .collect();
    if queries.len() < 2 {
        return Vec::new();
    }
    queries
        .into_iter()
        .take(max_pages.min(max_documents))
        .collect()
}

fn facet_document_ceiling(max_documents: usize, retained: usize, remaining_facets: usize) -> usize {
    retained + max_documents.saturating_sub(retained) / remaining_facets.max(1)
}

/// Whether `url` is a canonical page of an indexed site: HTTPS on developers.stellar.org or
/// stellar.org, with no credentials, port, or encoded path separators. Callers outside the
/// connector use it before fetching an indexed page again.
pub(crate) fn is_canonical_page(url: &str) -> bool {
    let Ok(url) = Url::parse(url) else {
        return false;
    };
    let path = url.path().to_lowercase();
    url.scheme() == "https"
        && matches!(
            url.host_str(),
            Some("developers.stellar.org" | "stellar.org")
        )
        && url.username().is_empty()
        && url.password().is_none()
        && url.port().is_none()
        && !path.contains('\\')
        && !path.contains("%2f")
        && !path.contains("%5c")
}

fn canonical(index: Index, hit: &Value) -> Option<Url> {
    let raw = hit
        .get("url_without_anchor")
        .and_then(Value::as_str)
        .or_else(|| hit.get("url").and_then(Value::as_str))?;
    // Reject schemes, credentials, ports, and off-site URLs before any original fetch.
    let mut url = if raw.starts_with('/') && !raw.starts_with("//") && index.scope == "SITE" {
        Url::parse("https://stellar.org").ok()?.join(raw).ok()?
    } else {
        Url::parse(raw).ok()?
    };
    let host = if index.scope == "DOCS" {
        "developers.stellar.org"
    } else {
        "stellar.org"
    };
    if url.scheme() != "https"
        || url.host_str() != Some(host)
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

async fn original(
    ctx: &FetchContext,
    source: &Source,
    index: Index,
    url: &Url,
) -> (Option<String>, Value, Vec<String>, FetchResult) {
    let mut failures = FetchResult::default();
    let result = &mut failures;
    if !index.production() {
        return (
            None,
            json!({"kind":"index_record","full_original":false,
            "limitation":"No published original fetch for development or staging records. Published content can differ."}),
            vec![],
            failures,
        );
    }
    let mut candidates = vec![];
    if index.scope == "DOCS" {
        let mut md = url.clone();
        if !md.path().ends_with(".md") {
            md.set_path(&format!("{}.md", md.path().trim_end_matches('/')));
        }
        candidates.push((md, "published_markdown"));
    }
    candidates.push((url.clone(), "original_html"));
    let mut artifacts = vec![];
    // A server error or a failed transfer is lost evidence; a missing page is not.
    let mut server_failed = false;
    for (candidate, kind) in candidates {
        match ctx
            .http
            .request(Method::GET, candidate.as_str(), vec![], None)
            .await
        {
            Ok(response) => {
                server_failed |= response.status >= 500;
                artifacts.push(response.artifact.clone());
                let text = response.text();
                let html = text
                    .trim_start()
                    .to_ascii_lowercase()
                    .starts_with("<!doctype html")
                    || text.trim_start().to_ascii_lowercase().starts_with("<html");
                if response.status == 200
                    && !text.trim().is_empty()
                    && (kind != "published_markdown"
                        || (!html && !unresolved_markdown_component(&text)))
                {
                    let extracted = if kind == "published_markdown" {
                        markdown_for_scoring(&text)
                            .map(|text| (text, "published_markdown_main_content"))
                    } else {
                        html_article_text(&text)
                    };
                    if let Some((scoring_text, scope)) = extracted {
                        return (
                            Some(scoring_text),
                            json!({"kind":kind,"full_original":true,"fetched_url":candidate.as_str(),
                                "content_scope":scope,"raw_body_chars":text.chars().count(),
                                "extraction_limitations":[
                                    "The raw artifact preserves the complete returned body. Scoring text is an extracted representation.",
                                    "Extraction does not execute scripts or external CSS. Dynamic and externally hidden content remains uncertain.",
                                    "Markdown can omit generated components. HTML extraction can omit non-text media and layout information."]}),
                            artifacts,
                            failures,
                        );
                    }
                }

                failure(
                    result,
                    source,
                    "original",
                    format!(
                        "Original request returned HTTP {}, an unusable body, or unresolved Markdown components. Artifact: {}",
                        response.status, response.artifact
                    ),
                );
            }
            Err(_) => {
                server_failed = true;
                failure(
                    result,
                    source,
                    "original",
                    "Original request failed. The recorder retains available failure evidence.",
                )
            }
        }
    }
    if server_failed {
        failure(
            result,
            source,
            "original_lost",
            format!("The original page {url} failed with a server or transfer error. The indexed record stands in for it."),
        );
    }
    (
        None,
        json!({"kind":"index_record","full_original":false,"requested":true,
        "limitation":"The original document was unavailable. Indexed records can contain only sections or metadata."}),
        artifacts,
        failures,
    )
}

/// The search host of an Algolia source: its application's endpoint, when configured.
pub fn search_host(source: &Source) -> Option<String> {
    let index = indices().find(|index| index.id() == source.id)?;
    let app = std::env::var(format!("ALGOLIA_APPLICATION_ID_{}", index.scope))
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
            title:format!("Fixture: {}",index.name),url:if index.scope=="DOCS" {"https://developers.stellar.org/docs/tools/cli/install-cli".into()}else{"https://stellar.org/enterprise-fund".into()},
            text:"Offline Algolia fixture. This is synthetic content, not a live source response.".into(),
            provenance:json!({"fixture":true,"application":index.scope,"index":index.name,"content_kind":"fixture","full_original":false}),raw_artifacts:vec![] });
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
    let app = std::env::var(format!("ALGOLIA_APPLICATION_ID_{}", index.scope))
        .ok()
        .filter(|s| !s.is_empty());
    let key = std::env::var(format!("ALGOLIA_API_KEY_{}", index.scope))
        .ok()
        .filter(|s| !s.is_empty());
    let (app, key) = match (app, key) {
        (Some(app), Some(key)) => (app, key),
        _ => {
            failure(
                &mut result,
                source,
                "authentication",
                format!("Missing Algolia {} read credentials.", index.scope),
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
    let docs_queries = if index.scope == "DOCS" {
        docs_facet_queries(&query_plan, ctx.config.max_pages, ctx.config.max_documents)
    } else {
        Vec::new()
    };
    let facet_mode = !docs_queries.is_empty();
    if facet_mode {
        let mut seen_facets = HashSet::new();
        for omitted in query_plan
            .keyword()
            .into_iter()
            .filter(|variant| variant.facet.is_some_and(|facet| seen_facets.insert(facet)))
            .filter(|variant| !docs_queries.contains(&variant.text))
        {
            failure(
                &mut result,
                source,
                "query_plan",
                format!(
                    "The query or document limit omitted a DOCS facet: {}",
                    omitted.text
                ),
            );
        }
    }
    let size = if facet_mode {
        ctx.config.max_documents.saturating_mul(3).min(100)
    } else {
        ctx.config.max_documents.min(100)
    };
    let mut page = 0;
    let mut relaxed = false;
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
            let document_ceiling = if facet_mode {
                let Some(query) = docs_queries.get(request_number) else {
                    break;
                };
                active_query = query.clone();
                query_kind = "keyword_facet";
                page = 0;
                facet_document_ceiling(
                    ctx.config.max_documents,
                    result.documents.len(),
                    docs_queries.len() - request_number,
                )
            } else {
                ctx.config.max_documents
            };
            let params = search_params(&active_query, page, size, relaxed);
            variants.push(json!({"query":active_query,"page":page,"matching_mode":if relaxed {"allOptional"} else {"none"},
                "kind":query_kind,"variant":if facet_mode {"docs_independent_facet"} else if keyword_retry {"site_shared_keyword_or_local_entity"} else if relaxed {"docs_optional_words"} else {"shared_initial"}}));
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
                if result.documents.len() >= document_ceiling {
                    // A later facet can still admit this record into its reserved capacity.
                    seen.remove(id);
                    if facet_mode && request_number + 1 < docs_queries.len() {
                        failure(&mut result, source, "query_facet_limit",
                            "The connector reserved document capacity for later DOCS facets. Raw responses retain omitted candidates.");
                        break;
                    }
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
                    provenance: json!({"application":index.scope,"index":index.name,"replica":index.replica(),"production":index.production(),
                        "query":question,"query_variant":{"kind":query_kind,"text":active_query},"query_used":active_query,"query_variants":variants,"relaxed_query":relaxed,"search_page":page,"records":[hit_provenance(hit)],"content_scope":original_meta.get("content_scope").cloned().unwrap_or(json!("indexed_sections_or_metadata")),
                        "extraction_limitations":original_meta.get("extraction_limitations").cloned().unwrap_or(json!(["Only indexed sections or metadata are available."])),"original":original_meta,
                        "index_metadata":null,"index_age_seconds":null,"index_inventory_verified_at_unix":null,
                        "limitations":["Index timestamps do not prove document freshness or corpus completeness.",
                            "Search can omit documents. Replicas do not supply independent corroboration."]}),
                    raw_artifacts: artifacts,
                };
                pages_by_url.insert(group, result.documents.len());
                result.documents.push(doc);
            }
            if facet_mode {
                if body
                    .get("nbPages")
                    .and_then(Value::as_u64)
                    .is_some_and(|n| n > 1)
                {
                    failure(&mut result, source, "query_facet_limit",
                        "The bounded DOCS facet search retained one response page. Additional matching records remain unread.");
                }
                // Each independent intent gets its reserved capacity before the local cap ends retrieval.
                if request_number + 1 < docs_queries.len() {
                    continue;
                }
                break;
            }
            if hits.is_empty()
                && page == 0
                && !keyword_retry
                && index.scope == "SITE"
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
            if hits.is_empty()
                && page == 0
                && !relaxed
                && index.scope == "DOCS"
                && total == Some(0)
                && body.get("message").is_none()
                && request_number + 1 < ctx.config.max_pages
            {
                relaxed = true;
                failure(
                    &mut result,
                    source,
                    "query_relaxation",
                    "The strict search returned no hits. The next query permits optional words.",
                );
                continue;
            }
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
                .buffered(ORIGINAL_READS)
                .collect()
                .await;
        for (position, (text, meta, artifacts, mut failures)) in originals {
            result.failures.append(&mut failures.failures);
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
        assert_eq!(indices().count(), 20);
        assert_eq!(sources.len(), 7);
        assert_eq!(
            sources.iter().map(|s| &s.id).collect::<HashSet<_>>().len(),
            7
        );
    }
    #[test]
    fn spanish_originals_use_the_verified_locale_prefix() {
        let index = Index {
            scope: "SITE",
            name: "pages_es",
        };
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
        let index = Index {
            scope: "SITE",
            name: "pages",
        };
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
        let p = search_params("q", 1, 20, false);
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
            planned_query(
                Index {
                    scope: "DOCS",
                    name: DOCS[0]
                },
                &plan
            ),
            ("rotate expired Widgetd signing keys".into(), "keywords")
        );
        let spanish = crate::query::plan(
            "¿Dónde encuentro documentación sobre límites y firmas de transacciones de Widgetd?",
        );
        assert_eq!(
            planned_query(
                Index {
                    scope: "SITE",
                    name: "pages_es"
                },
                &spanish
            )
            .0,
            spanish.keyword_text()
        );
        assert_eq!(
            planned_query(
                Index {
                    scope: "SITE",
                    name: "pages"
                },
                &spanish
            ),
            ("Widgetd".into(), "entity")
        );
    }
    #[test]
    fn docs_plan_separates_independent_intents_before_optional_word_noise() {
        let plan = crate::query::plan(
            "Find sources on rotating signer keys and recovering locked accounts.",
        );
        let queries = docs_facet_queries(&plan, 2, 3);
        assert_eq!(
            queries,
            ["rotating signer keys", "recovering locked accounts"]
        );
        for query in queries {
            let request = search_params(&query, 0, 9, false);
            assert_eq!(request["removeWordsIfNoResults"], "none");
            assert_eq!(request["analytics"], false);
        }
    }
    #[test]
    fn docs_facet_budget_reserves_both_intents_and_keeps_single_intent_behavior() {
        assert_eq!(facet_document_ceiling(3, 0, 2), 1);
        assert_eq!(facet_document_ceiling(3, 1, 1), 3);
        let broad = crate::query::plan("Find SEP-99 onboarding and SEP-98 token usage sources");
        let queries = docs_facet_queries(&broad, 2, 3);
        assert_eq!(queries.len(), 2);
        assert!(queries[0].contains("SEP-99"));
        assert!(queries[1].contains("SEP-98"));
        assert!(docs_facet_queries(&broad, 1, 3).is_empty());
        assert!(docs_facet_queries(&broad, 2, 1).is_empty());
        let single = crate::query::plan("How do I rotate expired Widgetd signing keys?");
        assert!(docs_facet_queries(&single, 2, 3).is_empty());
    }
    #[test]
    fn only_canonical_site_pages_are_fetched_again() {
        assert!(is_canonical_page("https://developers.stellar.org/docs/a"));
        assert!(is_canonical_page("https://stellar.org/blog/x"));
        for url in [
            "http://stellar.org/blog/x",
            "https://internal.example/a",
            "https://user:pw@stellar.org/a",
            "https://stellar.org:8443/a",
            "https://stellar.org/a%2F..%2Fb",
            "https://evil.stellar.org.example/a",
        ] {
            assert!(!is_canonical_page(url), "{url}");
        }
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
