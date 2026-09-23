//! Multi-criteria ranking of selected documents. Jev answers the semantic questions, asked the same
//! way of every question and document; this module does what Jev is documented to do poorly:
//! dates and the combination. Nothing here depends on the topic of the question.
use crate::types::{Document, DocumentScore};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
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

/// Per-document ranking inputs and results, serialized into search.json.
#[derive(Clone, Debug, Serialize)]
pub struct Ranked {
    pub id: String,
    pub bucket: u8,
    pub fused: f64,
    pub authority_tier: u8,
    pub date: Option<DocDate>,
    pub current: BTreeMap<String, f64>,
}

/// Intent weights for (relevance, currentness, recency, authority).
fn weights(intent: &Intent) -> [f64; 4] {
    match intent.kind.as_str() {
        "current" => [1.0, 1.0, 0.6, 0.5],
        "comparative" => [1.0, 0.6, 0.2, 0.5],
        "versioned" => [1.0, 0.7, 0.4, 0.6],
        _ if intent.versioned >= 0.5 => [1.0, 0.5, 0.3, 0.6],
        _ => [1.0, 0.0, 0.0, 0.3],
    }
}

/// Reciprocal-rank constant. Small, so top-rank differences survive in pools of 100+ documents.
const RRF_K: f64 = 10.0;

/// Order selected documents. Relevance (length-normalized) always counts; currentness, recency,
/// and authority count by intent. For a confident `current` intent, documents fall into buckets
/// first: 1 live and not superseded, 3 superseded, 2 everything else. The currentness answers come
/// from each document's best chunk.
pub fn rank(
    intent: &Intent,
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
    let currentness: Vec<f64> = (0..n)
        .map(|i| match current(i) {
            Some(a) => {
                let g = |k: &str| a.get(k).copied().unwrap_or(0.0);
                g("dated") + g("live") - g("planned_only") - g("superseded")
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
    let lists = [&relevance, &currentness, &recency, &authority];
    let ranks: Vec<Vec<f64>> = lists.iter().map(|l| positions(l)).collect();
    let fused: Vec<f64> = (0..n)
        .map(|i| (0..4).map(|c| w[c] / (RRF_K + ranks[c][i])).sum())
        .collect();
    let bucketed = intent.kind == "current" && intent.confidence >= BUCKET_CONFIDENCE;
    let bucket: Vec<u8> = (0..n)
        .map(|i| {
            if !bucketed {
                return 2;
            }
            let g = |k: &str| current(i).and_then(|a| a.get(k).copied());
            let superseded = g("superseded").unwrap_or(0.0) >= 0.5;
            let live = g("live").unwrap_or(0.0) >= 0.5;
            if superseded {
                3
            } else if live {
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
                &d("https://github.com/stellar/example-repo/releases"),
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
    fn a_confident_current_intent_puts_live_evidence_above_saturated_superseded_pages() {
        let old = doc(
            "old",
            "https://example.org/a",
            "Widget 1 guide",
            "Widget 1 setup",
            json!({"discovery":{"publishing_date":"2024-01-01"}}),
        );
        let neutral = doc(
            "neutral",
            "https://example.org/b",
            "Widget overview",
            "Widget facts",
            json!({}),
        );
        let live = doc(
            "live",
            "https://example.net/c",
            "Widget 2 is out",
            "Widget 2 shipped",
            json!({"row":{"publishedAt":"2026-01-02"}}),
        );
        let docs = [&old, &neutral, &live];
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
            score("neutral", 0.98),
            score("live", 0.95),
        ];
        s[0].current = current(0.9, 0.0, 0.8, 0.9);
        s[1].current = current(0.2, 0.3, 0.1, 0.1);
        s[2].current = current(0.95, 0.05, 0.0, 0.9);
        let scores: BTreeMap<&str, &DocumentScore> =
            s.iter().map(|x| (x.document_id.as_str(), x)).collect();
        let scopes = vec!["main_visible_text".to_owned(); 3];
        let intent = Intent {
            kind: "current".into(),
            confidence: 0.9,
            versioned: 0.9,
        };
        let ranked = rank(&intent, &docs, &scores, &scopes);
        let ids: Vec<&str> = ranked.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, ["live", "neutral", "old"]);
        assert_eq!(
            ranked.iter().map(|r| r.bucket).collect::<Vec<_>>(),
            [1, 2, 3]
        );
        // With low intent confidence there are no hard buckets.
        let unsure = Intent {
            confidence: 0.3,
            ..intent.clone()
        };
        assert!(rank(&unsure, &docs, &scores, &scopes)
            .iter()
            .all(|r| r.bucket == 2));
        // A timeless question ignores recency and currentness entirely.
        let timeless = Intent {
            kind: "timeless".into(),
            confidence: 0.9,
            versioned: 0.0,
        };
        let ranked = rank(&timeless, &docs, &scores, &scopes);
        assert_eq!(
            ranked[2].id, "live",
            "lower relevance stays last without time signals"
        );
    }

    #[test]
    fn edge_forms_of_dates_and_hosts() {
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
