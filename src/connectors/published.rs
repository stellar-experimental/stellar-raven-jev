//! Published pages that a search connector reads in full after its search: its own site's
//! Markdown or HTML page for each hit, through the source client, without source keys.
use crate::extract::{html_article_text, markdown_for_scoring, unresolved_markdown_component};
use crate::types::*;
use reqwest::{Method, Url};
use serde_json::{json, Value};

/// Pages read at once by one connector.
pub(crate) const READS: usize = 6;

/// The most same-host moves one page read follows.
const MAX_REDIRECTS: usize = 3;

/// Whether `url` is a canonical page of an indexed site: HTTPS on developers.stellar.org or
/// stellar.org, with no credentials, port, or encoded path separators. Callers outside the
/// connectors use it before fetching an indexed page again.
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

/// Where a page moved, when the move stays on the same host over HTTPS without user information.
fn same_host_move(from: &Url, location: &str) -> Option<Url> {
    let target = from.join(location).ok()?;
    (target.scheme() == "https"
        && target.host_str() == from.host_str()
        && target.port_or_known_default() == from.port_or_known_default()
        && target.username().is_empty()
        && target.password().is_none())
    .then_some(target)
}

/// One page read: the scoring text when a body was usable, the `original` provenance, the raw
/// artifacts, and the reports.
pub(crate) struct Read {
    pub text: Option<String>,
    pub meta: Value,
    pub artifacts: Vec<String>,
    pub failures: Vec<Failure>,
}

/// Read the page at `url`: first its published Markdown (`<url>.md`) when `markdown` is set, then
/// its HTML. A server or transfer error is lost evidence; a missing or moved page is not.
pub(crate) async fn read(ctx: &FetchContext, source: &Source, url: &Url, markdown: bool) -> Read {
    let mut failures = Vec::new();
    let mut failure = |stage: &str, message: String| {
        failures.push(Failure {
            stage: stage.into(),
            source_id: Some(source.id.clone()),
            message,
            cause: None,
        })
    };
    let mut candidates = vec![];
    if markdown {
        let mut md = url.clone();
        if !md.path().ends_with(".md") {
            md.set_path(&format!("{}.md", md.path().trim_end_matches('/')));
        }
        candidates.push((md, "published_markdown"));
    }
    candidates.push((url.clone(), "original_html"));
    let mut artifacts = vec![];
    let mut server_failed = false;
    let mut hops = 0;
    let mut next = 0;
    while next < candidates.len() {
        let (candidate, kind) = candidates[next].clone();
        next += 1;
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
                        return Read {
                            text: Some(scoring_text),
                            meta: json!({"kind":kind,"full_original":true,"fetched_url":candidate.as_str(),
                                "content_scope":scope,"raw_body_chars":text.chars().count(),
                                "extraction_limitations":[
                                    "The raw artifact preserves the complete returned body. Scoring text is an extracted representation.",
                                    "Extraction does not execute scripts or external CSS. Dynamic and externally hidden content remains uncertain.",
                                    "Markdown can omit generated components. HTML extraction can omit non-text media and layout information."]}),
                            artifacts,
                            failures,
                        };
                    }
                }
                failure(
                    "original",
                    format!(
                        "Original request returned HTTP {}, an unusable body, or unresolved Markdown components. Artifact: {}",
                        response.status, response.artifact
                    ),
                );
            }
            Err(error) => match error.downcast_ref::<crate::http::Redirected>() {
                Some(moved) => {
                    // A page that moved on the same host over HTTPS is read at its new location.
                    let target = moved
                        .location
                        .as_deref()
                        .and_then(|location| same_host_move(&candidate, location));
                    match target {
                        Some(target) if hops < MAX_REDIRECTS => {
                            hops += 1;
                            candidates.push((target, kind));
                        }
                        _ => failure(
                            "original",
                            format!(
                                "Original page moved (HTTP {}) to a location that is not read: another host, not HTTPS, missing, or past {MAX_REDIRECTS} moves.",
                                moved.status
                            ),
                        ),
                    }
                }
                None => {
                    server_failed = true;
                    failure(
                        "original",
                        "Original request failed. The recorder retains available failure evidence."
                            .into(),
                    )
                }
            },
        }
    }
    if server_failed {
        failure(
            "original_lost",
            format!("The original page {url} failed with a server or transfer error. The search record stands in for it."),
        );
    }
    Read {
        text: None,
        meta: json!({"kind":"index_record","full_original":false,"requested":true,
            "limitation":"The original document was unavailable. Search records can contain only sections or metadata."}),
        artifacts,
        failures,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn a_moved_original_is_followed_only_on_the_same_host_over_https() {
        let from = Url::parse("https://fernlet.test/connect").unwrap();
        assert_eq!(
            same_host_move(&from, "/community").unwrap().as_str(),
            "https://fernlet.test/community"
        );
        assert_eq!(
            same_host_move(&from, "https://fernlet.test/a?b=1")
                .unwrap()
                .as_str(),
            "https://fernlet.test/a?b=1"
        );
        for refused in [
            "https://quillon.test/community",
            "http://fernlet.test/community",
            "https://user:pw@fernlet.test/community",
            "https://fernlet.test:8443/community",
        ] {
            assert!(same_host_move(&from, refused).is_none(), "{refused}");
        }
    }
}
