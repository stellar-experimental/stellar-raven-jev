//! Original pages for source rows that describe a page without holding it. Eligibility comes from
//! source structure only: the row's content scope, its URL, and its source's API host. The reader
//! never looks at the question, never builds paths, and never follows links in what it reads.
use crate::{
    http::{HttpRecorder, Refused},
    types::{Document, Source},
};
use reqwest::Url;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

/// The source ID of pages the reader appends.
pub const SOURCE_ID: &str = "original";
const EXTRACTOR: &str = "original-reader-v1";
/// Row scopes that describe a page they do not contain.
const PARTIAL_SCOPES: &[&str] = &[
    "structured_record",
    "structured_record_with_detail",
    "catalog_metadata",
    "indexed_sections_or_metadata",
    "ai_summary",
];

/// A page to read: its normalized URL and the row that named it.
#[derive(Clone, Debug)]
pub struct Candidate {
    pub url: Url,
    pub parent_id: String,
    pub parent_source_id: String,
    pub title: String,
}

/// What planning decided for one eligible row.
#[derive(Debug)]
pub enum Plan {
    Read(Candidate),
    /// The session already holds a complete body for the URL; no request is needed.
    Reused {
        url: Url,
        document_id: String,
    },
}

#[derive(Debug)]
pub enum Outcome {
    /// A new document with source ID `original` and an ID the pipeline namespaces.
    Read(Document),
    /// The transport rules refused the read.
    Refused(String),
    /// The read failed or returned no usable text.
    Failed(String),
}

/// The URL identity of a page: HTTPS, no user information, a host name, port 443, no fragment.
/// Paths and queries stay as given.
pub fn normalize(url: &str) -> Option<Url> {
    let mut url = Url::parse(url.trim()).ok()?;
    let host = url.host_str()?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .is_ok()
    {
        return None;
    }
    url.set_fragment(None);
    Some(url)
}

/// The document ID of a read page, before the pipeline namespaces it with `original::`.
pub fn document_id(url: &Url) -> String {
    format!("{:x}", Sha256::digest(url.as_str().as_bytes()))[..16].to_owned()
}

/// A document that holds the complete body its URL returned.
fn complete_body(document: &Document) -> bool {
    document.provenance["full_original"] == true
        || document.provenance["original"]["full_original"] == true
}

/// The row's normalized URL when the row may be read: a partial scope, not synthetic, not a
/// staging record, not already requested by its connector, and a URL off its source's API host.
pub fn eligible(document: &Document, source: Option<&Source>) -> Option<Url> {
    let p = &document.provenance;
    if !PARTIAL_SCOPES.contains(&crate::search::content_scope(document))
        || p["production"] == false
        || p["original"]["requested"] == true
    {
        return None;
    }
    let url = normalize(&document.url)?;
    let api_host = source.and_then(super::search_host);
    (api_host.as_deref() != url.host_str()).then_some(url)
}

/// Eligible rows in the given order, one per normalized URL. A URL whose complete body the session
/// already holds (in `held` or among the rows) is reused, not read.
pub fn plan<'a>(
    rows: impl IntoIterator<Item = &'a Document>,
    held: impl IntoIterator<Item = &'a Document>,
    sources: &[Source],
) -> Vec<Plan> {
    let rows: Vec<&Document> = rows.into_iter().collect();
    let mut bodies: BTreeMap<String, String> = BTreeMap::new();
    for document in held.into_iter().chain(rows.iter().copied()) {
        if let Some(url) = complete_body(document)
            .then(|| normalize(&document.url))
            .flatten()
        {
            bodies
                .entry(url.to_string())
                .or_insert_with(|| document.id.clone());
        }
    }
    let mut seen = BTreeSet::new();
    let mut plans = Vec::new();
    for row in rows {
        let source = sources.iter().find(|s| s.id == row.source_id);
        let Some(url) = eligible(row, source) else {
            continue;
        };
        if !seen.insert(url.to_string()) {
            continue;
        }
        plans.push(match bodies.get(url.as_str()) {
            Some(id) => Plan::Reused {
                url,
                document_id: id.clone(),
            },
            None => Plan::Read(Candidate {
                url,
                parent_id: row.id.clone(),
                parent_source_id: row.source_id.clone(),
                title: row.title.clone(),
            }),
        });
    }
    plans
}

/// What extraction took from one body.
struct Extracted {
    text: Option<(String, &'static str)>,
    page_dates: Option<(Option<String>, Option<String>)>,
    bytes: usize,
    sha256: String,
}

/// Scoring text, page dates, size, and hash of a declared text body. A body that is not UTF-8 or
/// holds binary data is refused, with the reason.
fn extract(content_type: &str, bytes: Vec<u8>) -> Result<Extracted, String> {
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    let Ok(body) = std::str::from_utf8(&bytes) else {
        return Err("The body is not UTF-8 text".into());
    };
    if body.contains('\0') {
        return Err("The body contains binary data".into());
    }
    let (text, page_dates) = match content_type {
        "text/html" => (
            crate::extract::html_article_text(body),
            Some(crate::rank::html_page_dates(body)),
        ),
        "text/markdown" => (
            crate::extract::markdown_for_scoring(body)
                .map(|text| (text, "published_markdown_main_content")),
            None,
        ),
        _ => (
            Some(body.trim())
                .filter(|t| !t.is_empty())
                .map(|t| (t.to_owned(), "published_plain_text")),
            None,
        ),
    };
    Ok(Extracted {
        text,
        page_dates,
        bytes: bytes.len(),
        sha256,
    })
}

/// Read one page through a public reader (`HttpRecorder::public_reader`) and extract its text.
pub async fn read_original(candidate: &Candidate, http: &HttpRecorder) -> Outcome {
    let response = match http.get_public(candidate.url.as_str()).await {
        Ok(response) => response,
        Err(error) => {
            return match error.downcast_ref::<Refused>() {
                Some(refused) => Outcome::Refused(refused.to_string()),
                None => Outcome::Failed(error.to_string()),
            }
        }
    };
    if response.status != 200 {
        return Outcome::Failed(format!(
            "HTTP {}. Artifact: {}",
            response.status, response.artifact
        ));
    }
    let content_type = response
        .headers
        .get("content-type")
        .map(|v| {
            v.split(';')
                .next()
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
        })
        .unwrap_or_default();
    // Extraction is CPU work on untrusted text; it runs off the async workers.
    let kind = content_type.clone();
    let extracted = match tokio::task::spawn_blocking(move || extract(&kind, response.body)).await {
        Ok(Ok(extracted)) => extracted,
        Ok(Err(refusal)) => return Outcome::Refused(refusal),
        Err(error) => return Outcome::Failed(format!("Extraction stopped: {error}")),
    };
    let Some((text, scope)) = extracted.text else {
        return Outcome::Failed(format!(
            "The {content_type} body has no extractable text, or its structure passed the \
             extraction limits. Artifact: {}",
            response.artifact
        ));
    };
    let mut provenance = json!({
        "content_scope": scope, "full_original": true, "fetched_url": candidate.url.as_str(),
        "parent_document_id": candidate.parent_id, "parent_source_id": candidate.parent_source_id,
        "content_type": content_type, "raw_body_bytes": extracted.bytes,
        "body_sha256": extracted.sha256, "extractor": EXTRACTOR,
        "read_unix_ms": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default().as_millis() as u64,
        "extraction_limitations": [
            "A source row named this URL. The page is separate evidence; it does not verify the row.",
            "Scoring text is an extracted representation. Scripts, external CSS, and linked resources were not loaded, so dynamic or hidden content is uncertain.",
            "A complete HTTP response does not prove complete rendered content."],
    });
    if let Some((modified, published)) = extracted.page_dates {
        if modified.is_some() || published.is_some() {
            provenance["page_dates"] = json!({"modified": modified, "published": published});
        }
    }
    Outcome::Read(Document {
        id: document_id(&candidate.url),
        source_id: SOURCE_ID.into(),
        title: candidate.title.clone(),
        url: candidate.url.to_string(),
        text,
        provenance,
        raw_artifacts: vec![response.artifact],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: &str, url: &str, provenance: serde_json::Value) -> Document {
        Document {
            id: id.into(),
            source_id: "listing".into(),
            title: format!("Row {id}"),
            url: url.into(),
            text: "{}".into(),
            provenance,
            raw_artifacts: vec![],
        }
    }

    #[test]
    fn hostile_html_bodies_finish_extraction_quickly() {
        for body in [
            format!("<body>{}</body>", "<div>a".repeat(160_000)),
            format!("<body><main>{}</main></body>", "<meta a=\"".repeat(150_000)),
            format!("<body>{}</body>", "\"dateModified\": \"".repeat(100_000)),
        ] {
            let started = std::time::Instant::now();
            let extracted = extract("text/html", body.into_bytes()).ok().unwrap();
            let elapsed = started.elapsed();
            assert!(elapsed < std::time::Duration::from_secs(2), "{elapsed:?}");
            assert_eq!(extracted.sha256.len(), 64);
        }
    }

    #[test]
    fn normalized_urls_drop_fragments_and_default_ports_and_refuse_unsafe_forms() {
        let a = normalize("https://fernlet.test:443/guide?page=2#setup").unwrap();
        assert_eq!(a.as_str(), "https://fernlet.test/guide?page=2");
        for refused in [
            "http://fernlet.test/guide",
            "https://reader:secret@fernlet.test/guide",
            "https://fernlet.test:8443/guide",
            "https://127.0.0.1/guide",
            "https://[::1]/guide",
            "not a url",
        ] {
            assert!(normalize(refused).is_none(), "{refused}");
        }
    }

    #[test]
    fn eligibility_follows_scope_and_row_structure_only() {
        let scope = |s: &str| json!({"content_scope": s});
        assert!(eligible(
            &row("1", "https://fernlet.test/a", scope("structured_record")),
            None
        )
        .is_some());
        assert!(eligible(
            &row("2", "https://fernlet.test/a", scope("ai_summary")),
            None
        )
        .is_some());
        for full in [
            "research_chunk",
            "main_visible_text",
            "structured_roster",
            "",
        ] {
            assert!(eligible(&row("3", "https://fernlet.test/a", scope(full)), None).is_none());
        }
        let synthetic = json!({"content_scope":"structured_record","row":{"synthetic":true}});
        assert!(eligible(&row("4", "https://fernlet.test/a", synthetic), None).is_none());
        let staging = json!({"content_scope":"indexed_sections_or_metadata","production":false});
        assert!(eligible(&row("5", "https://fernlet.test/a", staging), None).is_none());
        let requested =
            json!({"content_scope":"indexed_sections_or_metadata","original":{"requested":true}});
        assert!(eligible(&row("6", "https://fernlet.test/a", requested), None).is_none());
        assert!(eligible(&row("7", "", scope("structured_record")), None).is_none());
    }

    #[test]
    fn plans_read_each_url_once_and_reuse_a_held_complete_body() {
        let scope = json!({"content_scope":"catalog_metadata"});
        let rows = [
            row("1", "https://fernlet.test/a#intro", scope.clone()),
            row("2", "https://fernlet.test:443/a", scope.clone()),
            row("3", "https://fernlet.test/b", scope.clone()),
        ];
        let held = [row(
            "held",
            "https://fernlet.test/b#top",
            json!({"content_scope":"main_visible_text","full_original":true}),
        )];
        let plans = plan(&rows, &held, &[]);
        assert_eq!(plans.len(), 2);
        let Plan::Read(first) = &plans[0] else {
            panic!("the first URL is read")
        };
        assert_eq!(first.url.as_str(), "https://fernlet.test/a");
        assert_eq!(first.parent_id, "1");
        assert!(matches!(&plans[1], Plan::Reused { document_id, .. } if document_id == "held"));
    }
}
