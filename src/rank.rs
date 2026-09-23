//! Multi-criteria ranking of selected documents. Jev answers the semantic questions; this module
//! does what Jev is documented to do poorly: dates, version order, counting, and the combination.
use crate::types::{Document, DocumentScore};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

/// The question's time intent from Jev's Choice. Hard currentness buckets need confidence.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Intent {
    pub kind: String,
    pub confidence: f64,
    pub versioned: f64,
}

/// Confidence at or above which a `current` intent applies hard buckets.
pub const BUCKET_CONFIDENCE: f64 = 0.6;

impl Intent {
    pub fn from_answers(answers: &BTreeMap<String, f64>) -> Self {
        let kind = crate::jev::INTENTS
            .iter()
            .map(|(option, _)| *option)
            .max_by(|a, b| {
                let p = |o: &str| answers.get(&format!("intent={o}")).copied().unwrap_or(0.0);
                p(a).total_cmp(&p(b))
            })
            .unwrap_or("timeless")
            .to_owned();
        Self {
            kind,
            confidence: answers.get("intent#confidence").copied().unwrap_or(0.0),
            versioned: answers.get("versioned").copied().unwrap_or(0.0),
        }
    }
    /// Scoring asks the currentness questions for every intent that depends on time or version.
    pub fn asks_currentness(&self) -> bool {
        self.kind != "timeless" || self.versioned >= 0.5
    }
}

/// A document date and what it means. Only `published` and `modified` drive recency.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct DocDate {
    pub date: String,
    pub kind: &'static str,
    #[serde(skip)]
    pub days: i64,
}

fn civil_days(y: i64, m: i64, d: i64) -> Option<i64> {
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) || !(1990..=2100).contains(&y) {
        return None;
    }
    // Days since 1970-01-01 (Howard Hinnant's algorithm).
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146097 + doe - 719468)
}

fn month_number(name: &str) -> Option<i64> {
    const MONTHS: [&str; 12] = [
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    ];
    let lower = name.to_ascii_lowercase();
    MONTHS
        .iter()
        .position(|m| lower.starts_with(m))
        .map(|i| i as i64 + 1)
}

/// Parse the first date in `text`: ISO `YYYY-MM-DD`, `Month D, YYYY`, or `DD/MM/YYYY`. A slashed
/// date whose first two fields are both 12 or less can be US or European order, so it is skipped.
fn parse_date(text: &str) -> Option<(String, i64)> {
    static PATTERNS: OnceLock<[Regex; 3]> = OnceLock::new();
    let [iso, named, slashed] = PATTERNS.get_or_init(|| {
        [
            Regex::new(r"\b(\d{4})-(\d{2})-(\d{2})").unwrap(),
            Regex::new(r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.? (\d{1,2}),? (\d{4})\b").unwrap(),
            Regex::new(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b").unwrap(),
        ]
    });
    let mut found: Vec<(usize, i64, i64, i64)> = Vec::new();
    if let Some(c) = iso.captures(text) {
        found.push((
            c.get(0)?.start(),
            c[1].parse().ok()?,
            c[2].parse().ok()?,
            c[3].parse().ok()?,
        ));
    }
    if let Some(c) = named.captures(text) {
        found.push((
            c.get(0)?.start(),
            c[3].parse().ok()?,
            month_number(&c[1])?,
            c[2].parse().ok()?,
        ));
    }
    if let Some(c) = slashed
        .captures(text)
        .filter(|c| c[1].parse::<u32>().unwrap_or(0) > 12 || c[2].parse::<u32>().unwrap_or(0) > 12)
    {
        found.push((
            c.get(0)?.start(),
            c[3].parse().ok()?,
            c[2].parse().ok()?,
            c[1].parse().ok()?,
        ));
    }
    found.sort();
    found.into_iter().find_map(|(_, y, m, d)| {
        civil_days(y, m, d).map(|days| (format!("{y:04}-{m:02}-{d:02}"), days))
    })
}

fn path<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .try_fold(value, |v, k| v.get(*k))
        .and_then(Value::as_str)
}

/// The document's own date, from provenance first, then from explicit labels in its text.
/// Index timestamps are never document dates. Upload and ingestion times are kept but do not drive
/// recency.
pub fn document_date(document: &Document) -> Option<DocDate> {
    let p = &document.provenance;
    let provenance = [
        (&["discovery", "publishing_date"][..], "published"),
        (&["row", "publishedAt"][..], "published"),
        (&["detail", "created_at"][..], "published"),
    ];
    let upload_source = document.source_id.starts_with("lumenloop.av");
    for (keys, kind) in provenance {
        if let Some((date, days)) = path(p, keys).and_then(parse_date) {
            return Some(DocDate { date, kind, days });
        }
    }
    // `created_at` is an upload time for recordings and an ingestion time where the connector says
    // so. Elsewhere (research, proposals) it is when the item was written.
    let ingested = path(p, &["publication_date_status"]).is_some_and(|s| s.contains("ingestion"));
    if let Some((date, days)) = path(p, &["discovery", "created_at"]).and_then(parse_date) {
        let kind = if upload_source {
            "upload_metadata"
        } else if ingested {
            "ingested"
        } else {
            "published"
        };
        return Some(DocDate { date, kind, days });
    }
    static LABELS: OnceLock<Regex> = OnceLock::new();
    let labels = LABELS.get_or_init(|| {
        Regex::new(r#"(?i)(last updated(?: on)?:?|publishing date:?|published(?: on)?:?|date:|datetime=")\s*"#).unwrap()
    });
    for m in labels.find_iter(&document.text) {
        let label = m.as_str().to_ascii_lowercase();
        let rest: String = document.text[m.end()..].chars().take(40).collect();
        if let Some((date, days)) = parse_date(&rest) {
            let kind = if label.contains("updated") || label.contains("datetime") {
                "modified"
            } else {
                "published"
            };
            return Some(DocDate { date, kind, days });
        }
    }
    None
}

/// Lowercase host without `www.`, and the URL path.
fn host_and_path(url: &str) -> Option<(String, String)> {
    let parsed = reqwest::Url::parse(url).ok()?;
    let host = parsed.host_str()?.to_ascii_lowercase();
    Some((
        host.trim_start_matches("www.").to_owned(),
        parsed.path().to_owned(),
    ))
}

fn host(url: &str) -> Option<String> {
    host_and_path(url).map(|(host, _)| host)
}

/// Authority tier: 1 official primary, 2 affiliated or first-party repositories, 3 editorial and
/// research, 4 summaries, synthetic records, and social posts. Tiers describe the source, not truth.
pub fn authority_tier(document: &Document, content_scope: &str) -> u8 {
    if matches!(content_scope, "ai_summary" | "synthetic_record") {
        return 4;
    }
    let (host, path) = host_and_path(&document.url).unwrap_or_default();
    match host.as_str() {
        "developers.stellar.org" | "stellar.org" => 1,
        "github.com" if path.starts_with("/stellar/") => 1,
        "x.com" | "twitter.com" | "youtube.com" | "youtu.be" | "medium.com" => 4,
        "skills.stellar.org" | "communityfund.stellar.org" | "github.com" => 2,
        _ => 3,
    }
}

fn protocol_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN
        .get_or_init(|| Regex::new(r"(?i)\bprotocol[oe]?(?:\s+version)?\s+v?(\d{1,3})\b").unwrap())
}

/// Protocol numbers the text mentions, as in "Protocol 28", "protocol version 28", or the Spanish
/// "Protocolo 28".
pub fn protocol_versions(text: &str) -> BTreeSet<u32> {
    protocol_pattern()
        .captures_iter(text)
        .filter_map(|c| c[1].parse().ok())
        .collect()
}

/// Whether a document is about the target version, decided in code: the title names it, the best
/// chunk names it at least twice, or the best chunk names it and it is the newest version the
/// document mentions.
fn about_target(document: &Document, score: Option<&DocumentScore>, target: u32) -> bool {
    let chunk = score
        .and_then(|s| document.text.get(s.best_chunk[0]..s.best_chunk[1]))
        .filter(|c| !c.is_empty())
        .unwrap_or(&document.text);
    let mentions = protocol_pattern()
        .captures_iter(chunk)
        .filter(|c| c[1].parse::<u32>().ok() == Some(target))
        .count();
    let newest = protocol_versions(&document.text).last() == Some(&target);
    protocol_versions(&document.title).contains(&target)
        || mentions >= 2
        || (newest && mentions >= 1)
}

/// Provenance clusters: documents that share a host or an identical normalized title count once.
pub fn clusters(documents: &[&Document]) -> Vec<usize> {
    let mut parent: Vec<usize> = (0..documents.len()).collect();
    fn find(parent: &mut [usize], i: usize) -> usize {
        let mut root = i;
        while parent[root] != root {
            root = parent[root];
        }
        parent[i] = root;
        root
    }
    let mut first: BTreeMap<String, usize> = BTreeMap::new();
    for (i, document) in documents.iter().enumerate() {
        let title: String = document
            .title
            .to_ascii_lowercase()
            .chars()
            .filter(|c| c.is_alphanumeric())
            .collect();
        let keys = [
            host(&document.url).map(|h| format!("host:{h}")),
            (title.len() >= 12).then(|| format!("title:{title}")),
            document
                .url
                .is_empty()
                .then(|| format!("source:{}", document.source_id)),
        ];
        for key in keys.into_iter().flatten() {
            match first.get(&key) {
                Some(&j) => {
                    let (a, b) = (find(&mut parent, i), find(&mut parent, j));
                    parent[a] = b;
                }
                None => {
                    first.insert(key, i);
                }
            }
        }
    }
    (0..documents.len()).map(|i| find(&mut parent, i)).collect()
}

/// The leading protocol version among the given documents: the highest one mentioned by at least
/// two provenance clusters or by one tier-1 document. A single mention cannot set the target. When
/// the documents carry currentness answers, at least one live document must also be about the
/// version, so a next protocol that official pages list as planned cannot become the target.
pub fn leading_protocol(
    documents: &[&Document],
    tiers: &[u8],
    scores: &BTreeMap<&str, &DocumentScore>,
) -> Option<(u32, usize)> {
    let score = |d: &Document| scores.get(d.id.as_str()).copied();
    let assessed = documents
        .iter()
        .any(|d| score(d).is_some_and(|s| !s.current.is_empty()));
    let live_about = |version: u32| {
        documents.iter().any(|d| {
            score(d).is_some_and(|s| s.current.get("live").copied().unwrap_or(0.0) >= 0.5)
                && about_target(d, score(d), version)
        })
    };
    let cluster_ids = clusters(documents);
    let mut support: BTreeMap<u32, (BTreeSet<usize>, bool)> = BTreeMap::new();
    for (i, document) in documents.iter().enumerate() {
        for version in protocol_versions(&document.text) {
            let entry = support.entry(version).or_default();
            entry.0.insert(cluster_ids[i]);
            entry.1 |= tiers[i] == 1;
        }
    }
    support
        .into_iter()
        .rev()
        .find(|(version, (clusters, official))| {
            (clusters.len() >= 2 || *official) && (!assessed || live_about(*version))
        })
        .map(|(version, (clusters, _))| (version, clusters.len()))
}

/// Per-document ranking inputs and results, serialized into search.json.
#[derive(Clone, Debug, Serialize)]
pub struct Ranked {
    pub id: String,
    pub bucket: u8,
    pub fused: f64,
    pub authority_tier: u8,
    pub date: Option<DocDate>,
    pub protocols: BTreeSet<u32>,
    pub about_target: bool,
    pub current: BTreeMap<String, f64>,
}

/// Intent weights for (relevance, currentness, recency, authority, corroboration).
fn weights(intent: &Intent) -> [f64; 5] {
    match intent.kind.as_str() {
        "current" => [1.0, 1.0, 0.6, 0.5, 0.3],
        "comparative" => [1.0, 0.6, 0.2, 0.5, 0.2],
        "versioned" => [1.0, 0.7, 0.4, 0.6, 0.2],
        _ if intent.versioned >= 0.5 => [1.0, 0.5, 0.3, 0.6, 0.2],
        _ => [1.0, 0.0, 0.0, 0.3, 0.0],
    }
}

/// Reciprocal-rank constant. Small, so top-rank differences survive in pools of 100+ documents.
const RRF_K: f64 = 10.0;

/// Order selected documents. Relevance (length-normalized) always counts; currentness, recency,
/// authority, and corroboration count by intent. For a confident `current` intent, documents fall
/// into buckets first: 1 live and about the leading target (or live when there is no target) and
/// not superseded, 3 superseded or about older versions only, 2 everything else.
/// The currentness answers come from each document's best chunk.
pub fn rank(
    intent: &Intent,
    target: Option<u32>,
    documents: &[&Document],
    scores: &BTreeMap<&str, &DocumentScore>,
    scopes: &[String],
) -> Vec<Ranked> {
    let n = documents.len();
    let tiers: Vec<u8> = documents
        .iter()
        .zip(scopes)
        .map(|(d, s)| authority_tier(d, s))
        .collect();
    let dates: Vec<Option<DocDate>> = documents.iter().map(|d| document_date(d)).collect();
    let protocols: Vec<BTreeSet<u32>> = documents
        .iter()
        .map(|d| protocol_versions(&d.text))
        .collect();
    let cluster_ids = clusters(documents);
    let relevance: Vec<f64> = documents
        .iter()
        .map(|d| {
            scores
                .get(d.id.as_str())
                .map(|s| (s.usable_top2_mean * 100.0).round())
                .unwrap_or(-1.0)
        })
        .collect();
    let score = |i: usize| scores.get(documents[i].id.as_str()).copied();
    let current = |i: usize| score(i).map(|s| &s.current).filter(|c| !c.is_empty());
    let about: Vec<bool> = (0..n)
        .map(|i| target.is_some_and(|t| about_target(documents[i], score(i), t)))
        .collect();
    let currentness: Vec<f64> = (0..n)
        .map(|i| match current(i) {
            Some(a) => {
                let g = |k: &str| a.get(k).copied().unwrap_or(0.0);
                f64::from(u8::from(about[i])) + g("dated") + g("live")
                    - g("planned_only")
                    - g("superseded")
            }
            None => f64::NEG_INFINITY,
        })
        .collect();
    let recency: Vec<f64> = dates
        .iter()
        .map(|d| match d {
            Some(d) if matches!(d.kind, "published" | "modified") => d.days as f64,
            _ => f64::NAN,
        })
        .collect();
    let authority: Vec<f64> = tiers.iter().map(|t| -(*t as f64)).collect();
    let corroboration: Vec<f64> = (0..n)
        .map(|i| match target {
            Some(t) if protocols[i].contains(&t) => {
                let supporting: BTreeSet<usize> = (0..n)
                    .filter(|j| protocols[*j].contains(&t))
                    .map(|j| cluster_ids[j])
                    .collect();
                supporting.len() as f64
            }
            _ => 0.0,
        })
        .collect();
    // Rank positions, higher value first. Ties share a position. Unknown recency takes the middle.
    let positions = |values: &[f64]| -> Vec<f64> {
        let known: Vec<f64> = values.iter().copied().filter(|v| !v.is_nan()).collect();
        values
            .iter()
            .map(|v| {
                if v.is_nan() {
                    (known.len() as f64 / 2.0).floor() + 1.0
                } else {
                    known.iter().filter(|o| **o > *v).count() as f64 + 1.0
                }
            })
            .collect()
    };
    let w = weights(intent);
    let lists = [
        &relevance,
        &currentness,
        &recency,
        &authority,
        &corroboration,
    ];
    let ranks: Vec<Vec<f64>> = lists.iter().map(|l| positions(l)).collect();
    let fused: Vec<f64> = (0..n)
        .map(|i| (0..5).map(|c| w[c] / (RRF_K + ranks[c][i])).sum())
        .collect();
    let bucketed = intent.kind == "current" && intent.confidence >= BUCKET_CONFIDENCE;
    let bucket: Vec<u8> = (0..n)
        .map(|i| {
            if !bucketed {
                return 2;
            }
            let g = |k: &str| current(i).and_then(|a| a.get(k).copied());
            let superseded = g("superseded").unwrap_or(0.0) >= 0.5;
            let older_only = match target {
                Some(t) => !protocols[i].is_empty() && protocols[i].iter().all(|v| *v < t),
                None => false,
            };
            let live = g("live").unwrap_or(0.0) >= 0.5;
            if superseded || older_only {
                3
            } else if (about[i] || target.is_none()) && live {
                1
            } else {
                2
            }
        })
        .collect();
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by(|&a, &b| {
        bucket[a]
            .cmp(&bucket[b])
            .then(fused[b].total_cmp(&fused[a]))
            .then(documents[a].id.cmp(&documents[b].id))
    });
    order
        .into_iter()
        .map(|i| Ranked {
            id: documents[i].id.clone(),
            bucket: bucket[i],
            fused: fused[i],
            authority_tier: tiers[i],
            date: dates[i].clone(),
            protocols: protocols[i].clone(),
            about_target: about[i],
            current: current(i).cloned().unwrap_or_default(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn doc(id: &str, url: &str, title: &str, text: &str, provenance: Value) -> Document {
        Document {
            id: id.into(),
            source_id: "s".into(),
            title: title.into(),
            url: url.into(),
            text: text.into(),
            provenance,
            raw_artifacts: vec![],
        }
    }
    fn score(id: &str, top2: f64) -> DocumentScore {
        DocumentScore {
            document_id: id.into(),
            probability: top2,
            reason: String::new(),
            signals: BTreeMap::new(),
            signals_aggregation: String::new(),
            best_chunk: [0, 0],
            usable_top2_mean: top2,
            current: BTreeMap::new(),
        }
    }

    #[test]
    fn dates_come_from_provenance_then_labels_and_never_from_index_metadata() {
        let p = doc(
            "a",
            "",
            "t",
            "",
            json!({"discovery":{"publishing_date":"2026-08-13 00:00:00+00"}}),
        );
        assert_eq!(document_date(&p).unwrap().date, "2026-08-13");
        let label = doc(
            "b",
            "",
            "t",
            "Blog\nLast updated on Aug 12, 2026 by X",
            json!({}),
        );
        let d = document_date(&label).unwrap();
        assert_eq!((d.date.as_str(), d.kind), ("2026-08-12", "modified"));
        let index_only = doc(
            "c",
            "",
            "t",
            "no dates",
            json!({"index_metadata":{"updatedAt":"2026-07-29T07:58:18Z"}}),
        );
        assert!(document_date(&index_only).is_none());
        let mut av = doc(
            "d",
            "",
            "t",
            "",
            json!({"discovery":{"created_at":"2026-04-28T05:30:30Z"}}),
        );
        av.source_id = "lumenloop.av".into();
        assert_eq!(document_date(&av).unwrap().kind, "upload_metadata");
        let scf = doc("e", "", "t", "Date: 31/08/2026", json!({}));
        assert_eq!(document_date(&scf).unwrap().date, "2026-08-31");
        assert!(civil_days(2026, 9, 16).unwrap() > civil_days(2026, 8, 13).unwrap());
    }

    #[test]
    fn authority_tiers_follow_source_and_scope() {
        let d = |url: &str| doc("x", url, "t", "", json!({}));
        assert_eq!(
            authority_tier(
                &d("https://developers.stellar.org/docs/x"),
                "published_markdown_main_content"
            ),
            1
        );
        assert_eq!(
            authority_tier(
                &d("https://github.com/stellar/js-stellar-sdk/releases"),
                "research_chunk"
            ),
            1
        );
        assert_eq!(
            authority_tier(&d("https://github.com/someone/repo"), "research_chunk"),
            2
        );
        assert_eq!(
            authority_tier(&d("https://lumenloop.com/research/x"), "research_chunk"),
            3
        );
        assert_eq!(
            authority_tier(&d("https://stellar.org/x"), "synthetic_record"),
            4
        );
        assert_eq!(
            authority_tier(&d("https://x.com/i/article/1"), "stored_editorial_body"),
            4
        );
    }

    #[test]
    fn leading_protocol_needs_two_clusters_or_an_official_page() {
        let a = doc(
            "a",
            "https://lumenloop.com/r/1",
            "Stellar Weekly Roundup week one",
            "Adapter (Protocol 28) activated",
            json!({}),
        );
        let b = doc(
            "b",
            "https://lumenloop.com/r/2",
            "Roundup two",
            "Protocol 28 is live",
            json!({}),
        );
        let c = doc(
            "c",
            "https://example.com/x",
            "Roadmap",
            "Protocol 29 ideas; Protocol 27 recap",
            json!({}),
        );
        let docs = [&a, &b, &c];
        // a and b share a host, so Protocol 28 has one cluster; Protocol 27 and 29 have one each.
        assert_eq!(leading_protocol(&docs, &[3, 3, 3], &BTreeMap::new()), None);
        // An official page mentioning Protocol 28 sets it; the lone Protocol 29 mention does not.
        let official = doc(
            "o",
            "https://stellar.org/blog/x",
            "Upgrade guide",
            "Protocol 28 upgrade guide",
            json!({}),
        );
        assert_eq!(
            protocol_versions("El Protocolo 23 y la protocol version 22"),
            BTreeSet::from([22, 23])
        );
        let docs = [&a, &b, &c, &official];
        assert_eq!(
            leading_protocol(&docs, &[3, 3, 3, 1], &BTreeMap::new()).map(|t| t.0),
            Some(28)
        );
        // Duplicate titles across hosts collapse into one cluster.
        let copy = doc(
            "d",
            "https://x.com/i/1",
            "Stellar Weekly Roundup week one",
            "Protocol 29",
            json!({}),
        );
        let separate = clusters(&[&c, &copy]);
        assert_ne!(separate[0], separate[1]);
        assert_eq!(clusters(&[&a, &copy])[0], clusters(&[&a, &copy])[1]);
    }

    #[test]
    fn a_confident_current_intent_puts_the_live_target_above_saturated_older_pages() {
        let older = doc(
            "old",
            "https://stellar.org/p23",
            "Protocol 23",
            "Protocol 23 Whisk upgrade guide",
            json!({"discovery":{"publishing_date":"2025-08-01"}}),
        );
        let stale = doc(
            "stale",
            "https://developers.stellar.org/v",
            "Software Versions",
            "Protocol 28 (Testnet, TBD). Protocol 27 (Mainnet)",
            json!({}),
        );
        let live = doc(
            "live",
            "https://lumenloop.com/r",
            "Roundup",
            "Adapter (Protocol 28) activated on Mainnet on September 16",
            json!({"row":{"publishedAt":"2026-09-18"}}),
        );
        let docs = [&older, &stale, &live];
        let current = |live: f64, planned_only: f64, superseded: f64, dated: f64| {
            BTreeMap::from([
                ("live".to_owned(), live),
                ("planned_only".to_owned(), planned_only),
                ("superseded".to_owned(), superseded),
                ("dated".to_owned(), dated),
            ])
        };
        let mut s = [
            score("old", 0.98),
            score("stale", 0.98),
            score("live", 0.95),
        ];
        s[0].current = current(0.9, 0.0, 0.2, 0.9);
        s[1].current = current(0.2, 0.8, 0.1, 0.1);
        s[2].current = current(0.95, 0.05, 0.0, 0.9);
        let scores: BTreeMap<&str, &DocumentScore> =
            s.iter().map(|x| (x.document_id.as_str(), x)).collect();
        let scopes = vec!["main_visible_text".to_owned(); 3];
        let intent = Intent {
            kind: "current".into(),
            confidence: 0.9,
            versioned: 0.9,
        };
        let ranked = rank(&intent, Some(28), &docs, &scores, &scopes);
        let ids: Vec<&str> = ranked.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, ["live", "stale", "old"]);
        assert_eq!(
            ranked.iter().map(|r| r.bucket).collect::<Vec<_>>(),
            [1, 2, 3]
        );
        assert_eq!(
            ranked.iter().map(|r| r.about_target).collect::<Vec<_>>(),
            [true, true, false],
            "the stale page names the target but is not live"
        );
        // With low intent confidence there are no hard buckets.
        let unsure = Intent {
            confidence: 0.3,
            ..intent.clone()
        };
        assert!(rank(&unsure, Some(28), &docs, &scores, &scopes)
            .iter()
            .all(|r| r.bucket == 2));
        // A timeless question ignores recency and currentness entirely.
        let timeless = Intent {
            kind: "timeless".into(),
            confidence: 0.9,
            versioned: 0.0,
        };
        let ranked = rank(&timeless, None, &docs, &scores, &scopes);
        assert_eq!(
            ranked[2].id, "live",
            "lower relevance stays last without time signals"
        );
    }

    #[test]
    fn a_next_protocol_that_no_live_document_covers_cannot_be_the_target() {
        let versions = doc(
            "v",
            "https://developers.stellar.org/docs/networks/software-versions",
            "Software Versions",
            "Protocol 29 (Testnet, TBD). Protocol 28 (Mainnet).",
            json!({}),
        );
        let roundup = doc(
            "r",
            "https://lumenloop.com/r",
            "Weekly Roundup",
            "Protocol 28 activated on Mainnet. Protocol 28 brings CAP-85.",
            json!({}),
        );
        let docs = [&versions, &roundup];
        let mut v = score("v", 0.9);
        v.current = BTreeMap::from([("live".to_owned(), 0.2)]);
        let mut r = score("r", 0.9);
        r.current = BTreeMap::from([("live".to_owned(), 0.9)]);
        let scores: BTreeMap<&str, &DocumentScore> = [("v", &v), ("r", &r)].into_iter().collect();
        assert_eq!(
            leading_protocol(&docs, &[1, 3], &scores).map(|t| t.0),
            Some(28)
        );
        // Without currentness answers, official support alone still sets the target.
        assert_eq!(
            leading_protocol(&docs, &[1, 3], &BTreeMap::new()).map(|t| t.0),
            Some(29)
        );
    }

    #[test]
    fn edge_forms_of_dates_versions_and_hosts() {
        let jobs = doc(
            "j",
            "https://x/j",
            "Job",
            "",
            json!({"discovery":{"created_at":"2026-09-18T20:08:08Z"},"publication_date_status":"not_returned; created_at_is_ingestion_time"}),
        );
        assert_eq!(document_date(&jobs).unwrap().kind, "ingested");
        let ambiguous = doc("a", "", "t", "Date: 03/04/2026", json!({}));
        assert_eq!(document_date(&ambiguous), None);
        let european = doc("e", "", "t", "Date: 23/04/2026", json!({}));
        assert_eq!(document_date(&european).unwrap().date, "2026-04-23");
        assert_eq!(
            protocol_versions("Protocol\n28 and protocol v27 and Protocol  26"),
            BTreeSet::from([26, 27, 28])
        );
        let upper = doc("u", "https://WWW.GitHub.com/stellar/x", "t", "", json!({}));
        assert_eq!(authority_tier(&upper, "main_visible_text"), 1);
    }

    #[test]
    fn intent_reads_the_flattened_choice() {
        let answers = BTreeMap::from([
            ("intent=current".to_owned(), 0.8),
            ("intent=timeless".to_owned(), 0.1),
            ("intent#confidence".to_owned(), 0.8),
            ("versioned".to_owned(), 0.7),
        ]);
        let intent = Intent::from_answers(&answers);
        assert_eq!(
            intent,
            Intent {
                kind: "current".into(),
                confidence: 0.8,
                versioned: 0.7
            }
        );
        assert!(intent.asks_currentness());
    }
}
