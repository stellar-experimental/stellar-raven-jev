//! Read-only retrieval of the Stellar developer docs through the Mintlify discovery API.
//!
//! The search returns the nearest page sections as a bare array: no scores, counts, or pages. It
//! returns sections even for a query the docs do not cover, so a response in which no section
//! holds a query term counts as empty. Each page found is then read whole from its published
//! Markdown on the docs site, which needs no key.
use super::published;
use crate::types::*;
use anyhow::Result;
use reqwest::{Method, Url};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

pub const SOURCE_ID: &str = "mintlify:docs";
const API: &str = "https://api.mintlify.com/discovery/v1";
/// The Mintlify project of the docs site.
const PROJECT: &str = "stellardevelopmentfoundation";
const SITE: &str = "https://developers.stellar.org";
const KEY: &str = "MINTLIFY_ASSISTANT_API_KEY";
/// Sections one search asks for: the most the API returns.
const CANDIDATES: usize = 50;
/// The 404 message that means the search found nothing. Any other 404 is a failure.
const NO_RESULTS: &str = "No results found";
/// Characters of an API error message kept in a report.
const MAX_ERROR_CHARS: usize = 200;
/// Time the fetch deadline must leave when a retry starts. The deadline can still cut it.
const RETRY_ROOM: std::time::Duration = std::time::Duration::from_secs(1);

pub fn sources() -> Vec<Source> {
    vec![Source {
        id: SOURCE_ID.into(),
        name: "Mintlify / Stellar developer docs".into(),
        description: "Official developer documentation and protocol meeting notes. Records contain headings or sections. English. Production index. Primary index.".into(),
        family: "mintlify".into(),
    }]
}

/// The host that search requests go to.
pub fn search_host() -> Option<String> {
    Some(Url::parse(API).ok()?.host_str()?.to_owned())
}

fn failure(result: &mut FetchResult, source: &Source, stage: &str, message: impl Into<String>) {
    result.failures.push(Failure {
        stage: stage.into(),
        source_id: Some(source.id.clone()),
        message: message.into(),
        cause: None,
    });
}

/// The query of a single search: the shared keyword text, or the entities of a question that is
/// not in English, since the docs are in English.
fn planned_query(plan: &crate::query::QueryPlan) -> (String, &'static str) {
    if plan.non_english {
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

/// One search per independent facet of the question, when the request and document limits allow
/// two or more. Empty otherwise.
fn facet_queries(
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

/// The documents a facet may hold: those already kept, plus an even share of what remains.
fn facet_document_ceiling(max_documents: usize, retained: usize, remaining_facets: usize) -> usize {
    retained + max_documents.saturating_sub(retained) / remaining_facets.max(1)
}

/// One section of a search response.
#[derive(Debug, PartialEq)]
struct Section {
    path: String,
    title: String,
    content: String,
}

/// What one search response says.
#[derive(Debug, PartialEq)]
enum Searched {
    Sections(Vec<Section>),
    /// The API says that it found nothing.
    Empty,
    Failed(String),
}

/// The `message` or `error` text of an API error body, bounded.
fn error_text(body: &[u8]) -> Option<String> {
    let value: Value = serde_json::from_slice(body).ok()?;
    ["message", "error"]
        .iter()
        .find_map(|key| value.get(key)?.as_str())
        .map(|text| text.chars().take(MAX_ERROR_CHARS).collect())
}

fn searched(status: u16, body: &[u8]) -> Searched {
    if status == 404 {
        let value: Option<Value> = serde_json::from_slice(body).ok();
        if value.as_ref().and_then(|v| v.get("message")?.as_str()) == Some(NO_RESULTS) {
            return Searched::Empty;
        }
    }
    if status != 200 {
        return Searched::Failed(match error_text(body) {
            Some(text) => format!("Search returned HTTP {status}: {text}"),
            None => format!("Search returned HTTP {status}."),
        });
    }
    let Ok(Value::Array(items)) = serde_json::from_slice::<Value>(body) else {
        return Searched::Failed("Search did not return an array of sections.".into());
    };
    let mut sections = Vec::with_capacity(items.len());
    for item in &items {
        let (Some(path), Some(content)) = (
            item.get("path").and_then(Value::as_str),
            item.get("content").and_then(Value::as_str),
        ) else {
            return Searched::Failed("A search section has no path or content.".into());
        };
        sections.push(Section {
            path: path.to_owned(),
            title: item
                .pointer("/metadata/title")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            content: content.to_owned(),
        });
    }
    Searched::Sections(sections)
}

/// The docs-site page of a section path. The site serves a folder's index page at the folder path.
/// Paths that leave the site, or that carry encoded separators, have no page.
fn page_url(path: &str) -> Option<Url> {
    let path = path.trim().trim_start_matches('/');
    if path.is_empty() || path.contains("://") || path.contains('?') || path.contains('#') {
        return None;
    }
    let mut url = Url::parse(SITE).ok()?.join(&format!("/{path}")).ok()?;
    let trimmed = url.path().trim_end_matches('/');
    let trimmed = trimmed.strip_suffix("/index").unwrap_or(trimmed).to_owned();
    url.set_path(if trimmed.is_empty() { "/" } else { &trimmed });
    (url.host_str() == Url::parse(SITE).ok()?.host_str()
        && published::is_canonical_page(url.as_str()))
    .then_some(url)
}

/// Lowercase words of `text`, each preceded by a space, for word-start matching.
fn normalized(text: &str) -> String {
    let mut out = String::from(" ");
    for c in text.chars() {
        if c.is_alphanumeric() {
            out.extend(c.to_lowercase());
        } else if !out.ends_with(' ') {
            out.push(' ');
        }
    }
    out
}

/// Terms of a query that a matching section holds: words of 3 or more characters or with a digit.
/// A word with inner punctuation counts whole and by its parts. "stellar" is on every page, so it
/// shows no match.
fn query_terms(query: &str) -> Vec<String> {
    let mut terms = Vec::new();
    for word in query.split_whitespace() {
        let parts: Vec<String> = normalized(word)
            .split_whitespace()
            .map(str::to_owned)
            .collect();
        let mut candidates = parts.clone();
        if parts.len() > 1 {
            candidates.insert(0, parts.join(" "));
        }
        for term in candidates {
            if term != "stellar"
                && (term.chars().count() >= 3 || term.chars().any(|c| c.is_ascii_digit()))
                && !terms.contains(&term)
            {
                terms.push(term);
            }
        }
    }
    terms
}

/// Whether a section holds a query term at a word start, in its title, its path after the first
/// segment, or its content. Every path starts with a section of the site (`docs/`, `meetings/`),
/// so that segment shows no match. A query without terms matches every section.
fn mentions(section: &Section, terms: &[String]) -> bool {
    if terms.is_empty() {
        return true;
    }
    let path = section
        .path
        .trim_start_matches('/')
        .split_once('/')
        .map_or("", |(_, rest)| rest);
    let haystack = normalized(&format!("{} {} {}", section.title, path, section.content));
    terms
        .iter()
        .any(|term| haystack.contains(&format!(" {term}")))
}

/// Section text without the site's code-fence theme settings.
fn clean_section(content: &str) -> String {
    content
        .lines()
        .map(|line| {
            let trimmed = line.trim_start();
            match trimmed.find(" theme={") {
                Some(at) if trimmed.starts_with("```") => &line[..line.len() - trimmed.len() + at],
                _ => line,
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The first heading of a section, when it starts with one. A closing `#` run is removed only
/// when a space comes before it, so `## Using C#` keeps its `#`.
fn heading(content: &str) -> Option<String> {
    let line = content.trim_start().lines().next()?;
    let text = line.trim_start_matches('#');
    if text.len() == line.len() || !text.starts_with(' ') {
        return None;
    }
    let text = text.trim();
    let open = text.trim_end_matches('#');
    let text = if open.len() < text.len() && (open.is_empty() || open.ends_with(' ')) {
        open.trim_end()
    } else {
        text
    };
    (!text.is_empty()).then(|| text.to_owned())
}

/// Published Markdown without the documentation-index notice that the site puts before each page:
/// the first block quote, when the page starts with one, and the blank lines after it. A later
/// block quote stays.
fn without_index_notice(text: &str) -> String {
    if !text.trim_start().starts_with('>') {
        return text.to_owned();
    }
    let rest: Vec<&str> = text
        .trim_start()
        .lines()
        .skip_while(|line| line.trim_start().starts_with('>'))
        .skip_while(|line| line.trim().is_empty())
        .collect();
    if rest.is_empty() {
        return text.to_owned();
    }
    rest.join("\n")
}

/// Whether a search outcome may be repeated: a server error, or a failure before a complete
/// response (a timeout too), unless the status that arrived first was a 4xx. A 4xx answer is
/// final, even when its body then fails, and so is a refusal by a rate limit. A failed 2xx or 3xx
/// body is repeated.
fn retryable(outcome: &anyhow::Result<crate::http::HttpResponse>) -> bool {
    match outcome {
        Ok(response) => response.status >= 500,
        Err(error) => error
            .downcast_ref::<crate::http::TransportFailure>()
            .is_some_and(|failure| {
                !failure
                    .status
                    .is_some_and(|status| (400..500).contains(&status))
            }),
    }
}

/// A short outcome of a search attempt, for a report.
fn outcome_text(outcome: &anyhow::Result<crate::http::HttpResponse>) -> String {
    match outcome {
        Ok(response) => format!("HTTP {}", response.status),
        Err(error) => error.to_string().trim_end_matches('.').to_owned(),
    }
}

/// Send one search, and repeat it once when [`retryable`], after 250 to 750 ms, if the fetch
/// deadline then still leaves [`RETRY_ROOM`]. The search is a read, so a repeat is safe. Returns
/// the last outcome and, when the first attempt failed, a note about it for a report.
async fn post_search(
    ctx: &FetchContext,
    url: &str,
    headers: &[(String, String)],
    body: &Value,
) -> (anyhow::Result<crate::http::HttpResponse>, Option<String>) {
    let first = ctx
        .http
        .request(Method::POST, url, headers.to_vec(), Some(body.clone()))
        .await;
    if !retryable(&first) {
        return (first, None);
    }
    let delay =
        std::time::Duration::from_millis(250 + (uuid::Uuid::new_v4().as_u128() % 500) as u64);
    let first_text = outcome_text(&first);
    if ctx.deadline.is_some_and(|deadline| {
        deadline.saturating_duration_since(tokio::time::Instant::now()) < delay + RETRY_ROOM
    }) {
        return (
            first,
            Some(format!(
                "It was not retried: the fetch deadline is too close. First attempt: {first_text}."
            )),
        );
    }
    let parent = first
        .as_ref()
        .ok()
        .map(|response| response.artifact.clone());
    tokio::time::sleep(delay).await;
    let retry = ctx
        .http
        .request_retry(
            Method::POST,
            url,
            headers.to_vec(),
            Some(body.clone()),
            parent,
            None,
        )
        .await;
    (retry, Some(format!("First attempt: {first_text}.")))
}

fn record(section: &Section, rank: usize, query: &str) -> Value {
    json!({"path":section.path,"title":section.title,"section":heading(&section.content),
        "search_rank":rank,"query":query})
}

pub async fn fetch(ctx: &FetchContext, source: &Source, question: &str) -> Result<FetchResult> {
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
        result.documents.push(Document {
            id: format!("{}:fixture", source.id),
            source_id: source.id.clone(),
            title: "Fixture: Stellar developer docs".into(),
            url: format!("{SITE}/docs/tools/cli/install-cli"),
            text: "Offline Mintlify fixture. This is synthetic content, not a live source response."
                .into(),
            provenance: json!({"fixture":true,"project":PROJECT,"content_kind":"fixture","full_original":false}),
            raw_artifacts: vec![],
        });
        return Ok(result);
    }
    let plan = crate::query::plan(question);
    for omission in &plan.omissions {
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
    let Some(key) = std::env::var(KEY).ok().filter(|s| !s.trim().is_empty()) else {
        failure(
            &mut result,
            source,
            "authentication",
            format!("Missing {KEY}."),
        );
        return Ok(result);
    };
    let headers = vec![("Authorization".to_owned(), format!("Bearer {}", key.trim()))];
    let url = format!("{API}/search/{PROJECT}");
    let pending = search(ctx, source, question, &plan, &url, headers, &mut result).await;
    // Read the pages together, bounded, and apply them in search order.
    use futures::StreamExt;
    let reads: Vec<_> =
        futures::stream::iter(pending)
            .map(|(position, url)| async move {
                (position, published::read(ctx, source, &url, true).await)
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
    ) in reads
    {
        result.failures.append(&mut failures);
        let doc = &mut result.documents[position];
        if let Some(text) = text {
            doc.text = without_index_notice(&text);
        }
        doc.provenance["content_scope"] = meta
            .get("content_scope")
            .cloned()
            .unwrap_or(json!("indexed_sections_or_metadata"));
        doc.provenance["extraction_limitations"] = meta
            .get("extraction_limitations")
            .cloned()
            .unwrap_or(json!(["Only search sections are available."]));
        doc.provenance["original"] = meta;
        doc.raw_artifacts.extend(artifacts);
    }
    Ok(result)
}

/// Send the searches of `plan` to `url` and keep one document per page, from its sections. Returns
/// the pages to read, by document position.
async fn search(
    ctx: &FetchContext,
    source: &Source,
    question: &str,
    plan: &crate::query::QueryPlan,
    url: &str,
    headers: Vec<(String, String)>,
    result: &mut FetchResult,
) -> Vec<(usize, Url)> {
    let facets = facet_queries(plan, ctx.config.max_pages, ctx.config.max_documents);
    let queries: Vec<(String, &str)> = if facets.is_empty() {
        vec![planned_query(plan)]
    } else {
        let mut seen = HashSet::new();
        for omitted in plan
            .keyword()
            .into_iter()
            .filter(|variant| variant.facet.is_some_and(|facet| seen.insert(facet)))
            .filter(|variant| !facets.contains(&variant.text))
        {
            failure(
                result,
                source,
                "query_plan",
                format!(
                    "The query or document limit omitted a facet: {}",
                    omitted.text
                ),
            );
        }
        facets
            .into_iter()
            .map(|query| (query, "keyword_facet"))
            .collect()
    };
    let mut variants = Vec::<Value>::new();
    let mut pages_by_url: HashMap<String, usize> = HashMap::new();
    let mut pending: Vec<(usize, Url)> = Vec::new();
    for (number, (query, kind)) in queries.iter().enumerate() {
        let ceiling = if queries.len() > 1 {
            facet_document_ceiling(
                ctx.config.max_documents,
                result.documents.len(),
                queries.len() - number,
            )
        } else {
            ctx.config.max_documents
        };
        variants.push(json!({"query":query,"kind":kind}));
        let body = json!({"query":query,"pageSize":CANDIDATES});
        let (outcome, note) = post_search(ctx, url, &headers, &body).await;
        let note = note.map(|note| format!(" {note}")).unwrap_or_default();
        let response = match outcome {
            Ok(response) => response,
            Err(error) => {
                failure(
                    result,
                    source,
                    "search",
                    format!(
                        "The search request failed: {}.{note} Partial results remain available.",
                        error.to_string().trim_end_matches('.')
                    ),
                );
                break;
            }
        };
        let sections = match searched(response.status, &response.body) {
            Searched::Sections(sections) if !sections.is_empty() => sections,
            Searched::Sections(_) | Searched::Empty => {
                failure(
                    result,
                    source,
                    "search_notice",
                    format!(
                        "The search returned no sections for {query:?}. This does not show that the docs lack the topic. Artifact: {}",
                        response.artifact
                    ),
                );
                continue;
            }
            Searched::Failed(message) => {
                failure(
                    result,
                    source,
                    "search",
                    format!("{message}{note} Artifact: {}", response.artifact),
                );
                break;
            }
        };
        let terms = query_terms(query);
        if !sections.iter().any(|section| mentions(section, &terms)) {
            failure(
                result,
                source,
                "search_notice",
                format!(
                    "None of the {} sections the search returned for {query:?} holds a query term ({}). The search returns its nearest sections even for a topic the docs do not cover, so they are not kept. Artifact: {}",
                    sections.len(),
                    terms.join(", "),
                    response.artifact
                ),
            );
            continue;
        }
        // Pages that hold a query term come first, each group in search order, so the document
        // limit cuts nearest pages without a term before pages with one. A later section joins
        // the page it belongs to.
        let pages: Vec<Option<Url>> = sections.iter().map(|s| page_url(&s.path)).collect();
        let holding: HashSet<String> = sections
            .iter()
            .zip(&pages)
            .filter(|(section, _)| mentions(section, &terms))
            .filter_map(|(_, page)| page.as_ref().map(Url::to_string))
            .collect();
        let mut order: Vec<usize> = (0..sections.len()).collect();
        order.sort_by_key(|&rank| {
            !pages[rank]
                .as_ref()
                .is_some_and(|page| holding.contains(page.as_str()))
        });
        let mut omitted = false;
        for rank in order {
            let section = &sections[rank];
            let Some(page) = pages[rank].clone() else {
                failure(
                    result,
                    source,
                    "record",
                    format!(
                        "A section path is not a docs-site page: {:?}",
                        section
                            .path
                            .chars()
                            .take(MAX_ERROR_CHARS)
                            .collect::<String>()
                    ),
                );
                continue;
            };
            let key = page.to_string();
            if let Some(&position) = pages_by_url.get(&key) {
                let doc = &mut result.documents[position];
                doc.provenance["records"]
                    .as_array_mut()
                    .unwrap()
                    .push(record(section, rank, query));
                doc.text.push_str("\n\n");
                doc.text.push_str(&clean_section(&section.content));
                if !doc.raw_artifacts.contains(&response.artifact) {
                    doc.raw_artifacts.push(response.artifact.clone());
                }
                continue;
            }
            if result.documents.len() >= ceiling {
                omitted = true;
                continue;
            }
            let title = if section.title.is_empty() {
                page.path()
                    .rsplit('/')
                    .next()
                    .unwrap_or_default()
                    .to_owned()
            } else {
                section.title.clone()
            };
            pending.push((result.documents.len(), page.clone()));
            pages_by_url.insert(key.clone(), result.documents.len());
            result.documents.push(Document {
                id: format!("{}:{}", source.id, page.path().trim_start_matches('/')),
                source_id: source.id.clone(),
                title,
                url: key,
                text: clean_section(&section.content),
                provenance: json!({"project":PROJECT,"query":question,
                    "query_variant":{"kind":kind,"text":query},"query_used":query,
                    "query_variants":variants,"records":[record(section, rank, query)],
                    "content_scope":"indexed_sections_or_metadata",
                    "extraction_limitations":["Only search sections are available."],
                    "original":{"kind":"index_record","full_original":false,"limitation":"The original is read after the search."},
                    "limitations":["The search returns its nearest sections without scores or counts. A page it did not return can still hold the answer."]}),
                raw_artifacts: vec![response.artifact.clone()],
            });
        }
        if omitted {
            let stage = if number + 1 < queries.len() {
                "query_facet_limit"
            } else {
                "limit"
            };
            failure(
                result,
                source,
                stage,
                "The document limit omitted pages the search returned. The search response retains them.",
            );
        }
    }
    pending
}

#[cfg(test)]
mod tests {
    use super::*;

    fn section(path: &str, title: &str, content: &str) -> Section {
        Section {
            path: path.into(),
            title: title.into(),
            content: content.into(),
        }
    }

    #[test]
    fn a_search_response_is_sections_empty_or_failed_by_its_body() {
        let ok =
            br#"[{"path":"docs/quillon/ledger","content":"Body","metadata":{"title":"Ledger"}}]"#;
        assert_eq!(
            searched(200, ok),
            Searched::Sections(vec![section("docs/quillon/ledger", "Ledger", "Body")])
        );
        assert_eq!(
            searched(404, br#"{"message":"No results found"}"#),
            Searched::Empty
        );
        let Searched::Failed(message) = searched(
            404,
            br#"{"error":"No deployment related to fernlet exists"}"#,
        ) else {
            panic!("another 404 is a failure");
        };
        assert!(
            message.contains("HTTP 404") && message.contains("No deployment"),
            "{message}"
        );
        assert!(matches!(
            searched(401, br#"{"error":"Unauthorized"}"#),
            Searched::Failed(_)
        ));
        assert!(matches!(
            searched(200, br#"{"hits":[]}"#),
            Searched::Failed(_)
        ));
        assert!(matches!(
            searched(200, br#"[{"path":"docs/a"}]"#),
            Searched::Failed(_)
        ));
        let long = format!(r#"{{"error":"{}"}}"#, "x".repeat(5_000));
        let Searched::Failed(message) = searched(500, long.as_bytes()) else {
            panic!();
        };
        assert!(message.len() < MAX_ERROR_CHARS + 40, "{}", message.len());
    }

    #[test]
    fn section_paths_become_site_pages_and_index_pages_lose_their_suffix() {
        assert_eq!(
            page_url("docs/quillon/receipts").unwrap().as_str(),
            "https://developers.stellar.org/docs/quillon/receipts"
        );
        assert_eq!(
            page_url("docs/quillon/index").unwrap().as_str(),
            "https://developers.stellar.org/docs/quillon"
        );
        assert_eq!(
            page_url("/meetings/2031/01/02/fernlet/").unwrap().as_str(),
            "https://developers.stellar.org/meetings/2031/01/02/fernlet"
        );
        for path in [
            "",
            "https://evil.test/docs/a",
            "docs/a%2F..%2Fb",
            "docs/a?b=1",
            "docs/a#b",
        ] {
            assert!(page_url(path).is_none(), "{path}");
        }
        // Other paths stay on the docs site.
        for path in ["//evil.test/docs/a", "docs/../../x", "docs\\a"] {
            assert_eq!(
                page_url(path).unwrap().host_str(),
                Some("developers.stellar.org"),
                "{path}"
            );
        }
    }

    #[test]
    fn a_response_without_a_query_term_holds_no_match() {
        let terms = query_terms("rotate Widgetd SEP-99 keys on Stellar");
        assert_eq!(terms, ["rotate", "widgetd", "sep 99", "sep", "99", "keys"]);
        assert!(mentions(
            &section("docs/a", "Widgetd setup", "text"),
            &terms
        ));
        assert!(mentions(&section("docs/sep-99", "", ""), &terms));
        assert!(mentions(
            &section("docs/a", "", "Rotated keys expire."),
            &query_terms("rotated")
        ));
        // A term matches at a word start only.
        assert!(!mentions(
            &section("docs/a", "Unrelated", "Stellar overview of corotate flows"),
            &query_terms("rotate Stellar")
        ));
        assert!(mentions(&section("docs/a", "", ""), &query_terms("a an")));
        // The first path segment is the site's section, shared by every row.
        assert!(!mentions(
            &section("docs/fernlet", "Fernlet", "Fernlet text"),
            &query_terms("docs")
        ));
        assert!(!mentions(
            &section("meetings/2031/01/02/x", "", ""),
            &query_terms("meetings")
        ));
        assert!(mentions(
            &section("docs/meetings-guide", "", ""),
            &query_terms("meetings")
        ));
    }

    #[test]
    fn section_text_drops_fence_themes_and_page_text_drops_the_index_notice() {
        let content = "## Install\n\n```sh theme={\"theme\":{\"light\":\"a\"}}\nquillon add\n```";
        assert_eq!(
            clean_section(content),
            "## Install\n\n```sh\nquillon add\n```"
        );
        assert_eq!(heading(content).as_deref(), Some("Install"));
        assert_eq!(heading("## Using C#").as_deref(), Some("Using C#"));
        assert_eq!(heading("## Receipts ##").as_deref(), Some("Receipts"));
        assert_eq!(heading("### Quillon #2 #").as_deref(), Some("Quillon #2"));
        assert_eq!(heading("Plain text"), None);
        let page = "> ## Documentation Index\n> Fetch the index.\n\n# Receipts\n\n> Batching receipts.\n\nBody";
        assert_eq!(
            without_index_notice(page),
            "# Receipts\n\n> Batching receipts.\n\nBody"
        );
        assert_eq!(
            without_index_notice("# Receipts\n\nBody"),
            "# Receipts\n\nBody"
        );
        // Only the first block quote is the notice; a later one is page text.
        assert_eq!(
            without_index_notice("> Notice.\n\n> A note that is page text.\n\n# Receipts"),
            "> A note that is page text.\n\n# Receipts"
        );
    }

    #[test]
    fn independent_facets_get_their_own_search_and_a_share_of_the_documents() {
        let plan = crate::query::plan(
            "Find sources on rotating signer keys and recovering locked accounts.",
        );
        assert_eq!(
            facet_queries(&plan, 2, 3),
            ["rotating signer keys", "recovering locked accounts"]
        );
        assert!(facet_queries(&plan, 1, 3).is_empty());
        assert!(facet_queries(&plan, 2, 1).is_empty());
        let single = crate::query::plan("How do I rotate expired Widgetd signing keys?");
        assert!(facet_queries(&single, 2, 3).is_empty());
        assert_eq!(
            planned_query(&single),
            ("rotate expired Widgetd signing keys".into(), "keywords")
        );
        assert_eq!(facet_document_ceiling(3, 0, 2), 1);
        assert_eq!(facet_document_ceiling(3, 1, 1), 3);
        let spanish = crate::query::plan(
            "¿Dónde encuentro documentación sobre límites y firmas de transacciones de Widgetd?",
        );
        assert_eq!(planned_query(&spanish), ("Widgetd".into(), "entity"));
    }

    /// A loopback server that answers each connection with the next of `replies`, and records
    /// each request.
    async fn replying(
        replies: Vec<String>,
    ) -> (
        std::net::SocketAddr,
        std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let log = seen.clone();
        tokio::spawn(async move {
            for reply in replies {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = vec![0; 8192];
                let n = socket.read(&mut request).await.unwrap_or(0);
                log.lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&request[..n]).into_owned());
                let _ = socket.write_all(reply.as_bytes()).await;
            }
        });
        (addr, seen)
    }

    fn reply(status: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    fn loopback(dir: &std::path::Path) -> FetchContext {
        let config = RunConfig {
            full_record: true,
            ..RunConfig::default()
        };
        FetchContext {
            http: crate::http::HttpRecorder::loopback_for_test(dir, &config).unwrap(),
            config,
            deadline: None,
        }
    }

    async fn search_once(
        reply_text: String,
        question: &str,
    ) -> (FetchResult, Vec<(usize, Url)>, String, String) {
        let (result, pending, requests, recorded) = search_with(vec![reply_text], question).await;
        (result, pending, requests.join("\n"), recorded)
    }

    /// Run `search` against a server that sends `replies` in order, and return what each request
    /// carried.
    async fn search_with(
        replies: Vec<String>,
        question: &str,
    ) -> (FetchResult, Vec<(usize, Url)>, Vec<String>, String) {
        search_within(replies, question, None, RunConfig::default().max_documents).await
    }

    /// `search_with`, with a fetch deadline `deadline` from now and a document limit.
    async fn search_within(
        replies: Vec<String>,
        question: &str,
        deadline: Option<std::time::Duration>,
        max_documents: usize,
    ) -> (FetchResult, Vec<(usize, Url)>, Vec<String>, String) {
        let (addr, seen) = replying(replies).await;
        let dir = tempfile::tempdir().unwrap();
        let mut ctx = loopback(dir.path());
        ctx.deadline = deadline.map(|d| tokio::time::Instant::now() + d);
        ctx.config.max_documents = max_documents;
        let mut result = FetchResult::default();
        let plan = crate::query::plan(question);
        let pending = search(
            &ctx,
            &sources()[0],
            question,
            &plan,
            &format!("http://{addr}/search/{PROJECT}"),
            vec![("Authorization".into(), "Bearer mint_dsc_test".into())],
            &mut result,
        )
        .await;
        let requests = seen.lock().unwrap().clone();
        let recorded: String = walk(dir.path())
            .into_iter()
            .map(|path| std::fs::read_to_string(path).unwrap_or_default())
            .collect();
        (result, pending, requests, recorded)
    }

    fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut files = Vec::new();
        for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                files.extend(walk(&path));
            } else {
                files.push(path);
            }
        }
        files
    }

    #[tokio::test]
    async fn sections_of_one_page_become_one_document_and_the_key_is_not_recorded() {
        let body = json!([
            {"path":"docs/quillon/receipts","content":"## Batching\nQuillon receipts batch hourly.","metadata":{"title":"Receipts"}},
            {"path":"https://evil.test/x","content":"Quillon","metadata":{"title":"Off site"}},
            {"path":"docs/quillon/index","content":"Quillon overview.","metadata":{"title":"Quillon"}},
            {"path":"docs/quillon/receipts","content":"## Limits\nA batch holds receipts.","metadata":{"title":"Receipts"}}
        ])
        .to_string();
        let (result, pending, request, recorded) =
            search_once(reply("200 OK", &body), "How does Quillon batch receipts?").await;
        let lower = request.to_ascii_lowercase();
        assert!(
            lower.starts_with("post /search/stellardevelopmentfoundation"),
            "{request}"
        );
        assert!(lower.contains("authorization: bearer mint_dsc_test"));
        assert!(request.contains(r#""pageSize":50"#), "{request}");
        assert!(!recorded.is_empty());
        assert!(!recorded.contains("mint_dsc_test"));
        let urls: Vec<_> = result.documents.iter().map(|d| d.url.as_str()).collect();
        assert_eq!(
            urls,
            [
                "https://developers.stellar.org/docs/quillon/receipts",
                "https://developers.stellar.org/docs/quillon"
            ]
        );
        let receipts = &result.documents[0];
        assert_eq!(receipts.id, "mintlify:docs:docs/quillon/receipts");
        assert_eq!(receipts.title, "Receipts");
        assert!(receipts.text.contains("batch hourly") && receipts.text.contains("A batch holds"));
        assert_eq!(receipts.provenance["records"].as_array().unwrap().len(), 2);
        assert_eq!(receipts.provenance["records"][1]["section"], "Limits");
        assert_eq!(
            receipts.provenance["content_scope"],
            "indexed_sections_or_metadata"
        );
        assert_eq!(pending.iter().map(|(i, _)| *i).collect::<Vec<_>>(), [0, 1]);
        assert!(result.failures.iter().any(|f| f.stage == "record"));
    }

    #[tokio::test]
    async fn nearest_sections_without_a_query_term_are_not_kept() {
        let body =
            json!([{"path":"docs/a","content":"Fernlet overview","metadata":{"title":"Fernlet"}}])
                .to_string();
        let (result, pending, _, _) =
            search_once(reply("200 OK", &body), "How does Quillon batch receipts?").await;
        assert!(result.documents.is_empty() && pending.is_empty());
        assert_eq!(result.failures[0].stage, "search_notice");
        assert!(result.failures[0].message.contains("holds a query term"));
    }

    #[tokio::test]
    async fn only_the_no_results_404_is_an_empty_search() {
        let (result, _, _, _) = search_once(
            reply("404 Not Found", r#"{"message":"No results found"}"#),
            "How does Quillon batch receipts?",
        )
        .await;
        assert!(result.documents.is_empty());
        assert_eq!(result.failures[0].stage, "search_notice");
        let (result, _, _, _) = search_once(
            reply(
                "404 Not Found",
                r#"{"error":"No deployment related to fernlet exists"}"#,
            ),
            "How does Quillon batch receipts?",
        )
        .await;
        assert_eq!(result.failures[0].stage, "search");
        assert!(result.failures[0].message.contains("No deployment"));
    }

    #[tokio::test]
    async fn a_server_error_is_retried_once_and_a_404_is_not() {
        let question = "How does Quillon batch receipts?";
        let ok = json!([{"path":"docs/quillon","content":"Quillon receipts","metadata":{"title":"Quillon"}}])
            .to_string();
        let (result, _, requests, recorded) = search_with(
            vec![reply("503 Service Unavailable", "{}"), reply("200 OK", &ok)],
            question,
        )
        .await;
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0], requests[1], "the retry repeats the request");
        assert_eq!(result.documents.len(), 1);
        assert!(result.failures.is_empty(), "{:?}", result.failures);
        assert!(recorded.contains(r#""role":"retry""#));

        let (result, _, requests, _) = search_with(
            vec![reply("404 Not Found", r#"{"message":"No results found"}"#)],
            question,
        )
        .await;
        assert_eq!(requests.len(), 1);
        assert_eq!(result.failures[0].stage, "search_notice");

        let (result, _, requests, _) = search_with(
            vec![
                reply("502 Bad Gateway", r#"{"error":"upstream"}"#),
                reply("503 Service Unavailable", r#"{"error":"busy"}"#),
            ],
            question,
        )
        .await;
        assert_eq!(requests.len(), 2);
        let failure = &result.failures[0];
        assert_eq!(failure.stage, "search");
        assert!(
            failure.message.contains("HTTP 503: busy"),
            "{}",
            failure.message
        );
        assert!(
            failure.message.contains("First attempt: HTTP 502"),
            "{}",
            failure.message
        );

        let (result, _, requests, _) = search_with(
            vec![reply("401 Unauthorized", r#"{"error":"Unauthorized"}"#)],
            question,
        )
        .await;
        assert_eq!(requests.len(), 1);
        assert_eq!(result.failures[0].stage, "search");
    }

    #[tokio::test]
    async fn a_failure_before_a_response_is_retried_unless_the_deadline_is_close() {
        let question = "How does Quillon batch receipts?";
        let ok = json!([{"path":"docs/quillon","content":"Quillon receipts","metadata":{"title":"Quillon"}}])
            .to_string();
        // An empty reply closes the connection without a response.
        let (result, _, requests, _) =
            search_with(vec![String::new(), reply("200 OK", &ok)], question).await;
        assert_eq!(requests.len(), 2);
        assert_eq!(result.documents.len(), 1);
        assert!(result.failures.is_empty(), "{:?}", result.failures);

        let (result, _, requests, _) = search_within(
            vec![reply("503 Service Unavailable", "{}"), reply("200 OK", &ok)],
            question,
            Some(std::time::Duration::from_millis(300)),
            RunConfig::default().max_documents,
        )
        .await;
        assert_eq!(requests.len(), 1);
        let failure = &result.failures[0];
        assert_eq!(failure.stage, "search");
        assert!(
            failure.message.contains("not retried"),
            "{}",
            failure.message
        );
    }

    #[tokio::test]
    async fn a_4xx_whose_body_fails_is_not_retried() {
        let ok = json!([{"path":"docs/quillon","content":"Quillon","metadata":{"title":"Q"}}])
            .to_string();
        for status in ["401 Unauthorized", "429 Too Many Requests"] {
            // The body is shorter than its declared length, so reading it fails.
            let incomplete = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: 200\r\nConnection: close\r\n\r\n{{}}"
            );
            let (result, _, requests, _) =
                search_with(vec![incomplete, reply("200 OK", &ok)], "Quillon receipts").await;
            assert_eq!(requests.len(), 1, "{status}");
            assert!(result.documents.is_empty());
            assert_eq!(result.failures[0].stage, "search", "{status}");
        }
        // A failed 200 body is not a 4xx answer, so it keeps its retry.
        let incomplete = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 200\r\nConnection: close\r\n\r\n[".to_owned();
        let (result, _, requests, _) =
            search_with(vec![incomplete, reply("200 OK", &ok)], "Quillon receipts").await;
        assert_eq!(requests.len(), 2);
        assert_eq!(result.documents.len(), 1);
        assert!(result.failures.is_empty(), "{:?}", result.failures);
    }

    #[tokio::test]
    async fn pages_that_hold_a_query_term_are_kept_before_nearer_pages_without_one() {
        let mut rows: Vec<Value> = (0..12)
            .map(|i| json!({"path":format!("docs/fernlet/{i}"),"content":"Fernlet overview","metadata":{"title":"Fernlet"}}))
            .collect();
        rows.push(json!({"path":"docs/fernlet/receipts","content":"Quillon batches receipts.","metadata":{"title":"Batching"}}));
        rows.push(json!({"path":"docs/fernlet/0","content":"More overview","metadata":{"title":"Fernlet"}}));
        let (result, pending, _, _) = search_within(
            vec![reply("200 OK", &Value::Array(rows).to_string())],
            "How does Quillon batch receipts?",
            None,
            12,
        )
        .await;
        assert_eq!(result.documents.len(), 12);
        assert_eq!(pending.len(), 12);
        assert_eq!(
            result.documents[0].url,
            "https://developers.stellar.org/docs/fernlet/receipts"
        );
        assert!(result
            .documents
            .iter()
            .all(|d| d.url != "https://developers.stellar.org/docs/fernlet/11"));
        // A later section joins a kept page, even at the limit.
        let first = result
            .documents
            .iter()
            .find(|d| d.url.ends_with("/fernlet/0"))
            .unwrap();
        assert_eq!(first.provenance["records"].as_array().unwrap().len(), 2);
        assert_eq!(
            result
                .failures
                .iter()
                .filter(|f| f.stage == "limit")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn fixture_runs_without_credentials_or_network() {
        let config = RunConfig {
            fixture: true,
            ..Default::default()
        };
        let dir = tempfile::tempdir().unwrap();
        let ctx = FetchContext {
            http: crate::http::HttpRecorder::new(dir.path(), &config).unwrap(),
            config,
            deadline: None,
        };
        let source = &sources()[0];
        let a = fetch(&ctx, source, "question").await.unwrap();
        assert_eq!(a.documents.len(), 1);
        assert_eq!(a.documents[0].provenance["fixture"], true);
        assert!(a.documents[0].raw_artifacts.is_empty());
    }
}
