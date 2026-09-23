//! Experimental evidence units. All ranges address the unchanged UTF-8 source text.
//! No segmentation or selection result proves that the question has complete support.
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::ops::Range;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Granularity {
    Document,
    Paragraph,
    Sentence,
    Window,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EvidenceUnit {
    pub target: Range<usize>,
    pub context: Range<usize>,
}

pub fn text_sha256(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

// Blank lines separate blocks. Fenced code remains indivisible, including blank lines.
fn blocks(text: &str) -> Vec<Range<usize>> {
    let mut result = Vec::new();
    let mut start = 0;
    let mut offset = 0;
    let mut fence: Option<(char, usize)> = None;
    let lines: Vec<_> = text.split_inclusive('\n').collect();
    let mut list = false;
    for (index, line) in lines.iter().enumerate() {
        let trimmed = line.trim_start();
        let marker = trimmed.chars().next().unwrap_or(' ');
        let count = trimmed.chars().take_while(|c| *c == marker).count();
        if matches!(marker, '`' | '~') && count >= 3 {
            match fence {
                None => fence = Some((marker, count)),
                Some((open, length))
                    if marker == open && count >= length && trimmed[count..].trim().is_empty() =>
                {
                    fence = None
                }
                _ => {}
            }
        }
        list |= list_item(trimmed);
        offset += line.len();
        if fence.is_none() && line.trim().is_empty() {
            // A blank line inside a loose list does not end the list. Keep following items
            // and indented continuation paragraphs with the preceding list context.
            if list
                && lines[index + 1..]
                    .iter()
                    .find(|next| !next.trim().is_empty())
                    .is_some_and(|next| {
                        list_item(next.trim_start())
                            || next.starts_with("  ")
                            || next.starts_with('\t')
                    })
            {
                continue;
            }
            if !text[start..offset].trim().is_empty() {
                result.push(start..offset);
            } else if let Some(previous) = result.last_mut() {
                previous.end = offset;
            }
            start = offset;
            list = false;
        }
    }
    if start < text.len() && !text[start..].trim().is_empty() {
        result.push(start..text.len());
    }
    result
}

fn list_item(value: &str) -> bool {
    ["- ", "* ", "+ "]
        .iter()
        .any(|prefix| value.starts_with(prefix))
        || [". ", ") "].iter().any(|separator| {
            value.split_once(separator).is_some_and(|(prefix, _)| {
                !prefix.is_empty() && prefix.bytes().all(|b| b.is_ascii_digit())
            })
        })
}

fn protected_block(text: &str) -> bool {
    text.lines().any(|line| {
        let value = line.trim_start();
        value.starts_with("```")
            || value.starts_with("~~~")
            || value.contains('|')
            || list_item(value)
            || value.starts_with("# ")
            || value.starts_with("##")
            || line.starts_with("    ")
    })
}

fn sentences(text: &str, block: &Range<usize>) -> Vec<Range<usize>> {
    let body = &text[block.clone()];
    if protected_block(body) {
        return vec![block.clone()];
    }
    let mut result = Vec::new();
    let mut start = block.start;
    for (index, ch) in body.char_indices() {
        if !matches!(ch, '.' | '?' | '!') {
            continue;
        }
        let end = block.start + index + ch.len_utf8();
        let rest = &text[end..block.end];
        if rest.is_empty() || !rest.starts_with(char::is_whitespace) {
            continue;
        }
        // Avoid common abbreviations and numbered labels. This is a heuristic, not a parser.
        let token = text[start..end].split_whitespace().last().unwrap_or("");
        if matches!(token, "e.g." | "i.e." | "Mr." | "Dr." | "vs." | "etc.")
            || (token.len() == 2 && token.as_bytes()[0].is_ascii_alphabetic())
            || token
                .trim_end_matches('.')
                .bytes()
                .all(|b| b.is_ascii_digit())
        {
            continue;
        }
        let whitespace = rest.len() - rest.trim_start().len();
        let boundary = end + whitespace;
        result.push(start..boundary);
        start = boundary;
    }
    if start < block.end {
        result.push(start..block.end);
    }
    result
}

/// Window scoring includes the complete target block and adjacent blocks.
/// Unlike a sentence-only arm, it can retain antecedents, exceptions, and nearby dates.
/// All arms keep tables, lists, and fenced code intact. Oversized units remain visible.
pub fn units(text: &str, granularity: Granularity) -> Vec<EvidenceUnit> {
    if text.trim().is_empty() {
        return Vec::new();
    }
    if granularity == Granularity::Document {
        return vec![EvidenceUnit {
            target: 0..text.len(),
            context: 0..text.len(),
        }];
    }
    let blocks = blocks(text);
    let mut result = Vec::new();
    for (index, block) in blocks.iter().enumerate() {
        let targets = if granularity == Granularity::Sentence {
            sentences(text, block)
        } else {
            vec![block.clone()]
        };
        for target in targets {
            let context = if granularity == Granularity::Window {
                blocks[index.saturating_sub(1)].start..blocks[(index + 1).min(blocks.len() - 1)].end
            } else {
                target.clone()
            };
            result.push(EvidenceUnit { target, context });
        }
    }
    result
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UnitScore {
    pub unit: EvidenceUnit,
    /// Missing scores remain unknown. They are never converted to zero.
    pub probability: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvidencePacket {
    pub text_sha256: String,
    pub source_bytes: usize,
    pub delivered_text_bytes: usize,
    pub byte_budget: usize,
    pub spans: Vec<Range<usize>>,
    pub selected_units: Vec<usize>,
    pub unscored_units: Vec<usize>,
    pub below_threshold_units: Vec<usize>,
    pub budget_omitted_units: Vec<usize>,
}

fn merged(mut ranges: Vec<Range<usize>>) -> Vec<Range<usize>> {
    ranges.sort_by_key(|range| range.start);
    let mut result: Vec<Range<usize>> = Vec::new();
    for range in ranges {
        if let Some(last) = result.last_mut() {
            if range.start <= last.end {
                last.end = last.end.max(range.end);
                continue;
            }
        }
        result.push(range);
    }
    result
}

/// Greedy score-order packing. The budget counts exact source bytes, excluding metadata.
/// Never crop a unit to fit. An oversized unit is an explicit omission, not a partial quotation.
pub fn pack(
    text: &str,
    scores: &[UnitScore],
    threshold: f64,
    byte_budget: usize,
) -> Result<EvidencePacket> {
    ensure!(
        threshold.is_finite() && (0.0..=1.0).contains(&threshold),
        "Invalid threshold"
    );
    ensure!(byte_budget > 0, "The evidence byte budget must be positive");
    let mut packet = EvidencePacket {
        text_sha256: text_sha256(text),
        source_bytes: text.len(),
        delivered_text_bytes: 0,
        byte_budget,
        spans: vec![],
        selected_units: vec![],
        unscored_units: vec![],
        below_threshold_units: vec![],
        budget_omitted_units: vec![],
    };
    let mut candidates = Vec::new();
    for (index, score) in scores.iter().enumerate() {
        let unit = &score.unit;
        ensure!(
            unit.context.start <= unit.target.start
                && unit.target.start < unit.target.end
                && unit.target.end <= unit.context.end
                && unit.context.end <= text.len()
                && [
                    unit.context.start,
                    unit.target.start,
                    unit.target.end,
                    unit.context.end
                ]
                .iter()
                .all(|position| text.is_char_boundary(*position)),
            "Invalid evidence byte range"
        );
        match score.probability {
            None => packet.unscored_units.push(index),
            Some(value) => {
                ensure!(
                    value.is_finite() && (0.0..=1.0).contains(&value),
                    "Invalid evidence score"
                );
                if value < threshold {
                    packet.below_threshold_units.push(index);
                } else {
                    candidates.push((index, value));
                }
            }
        }
    }
    candidates.sort_by(|(ai, a), (bi, b)| b.total_cmp(a).then(ai.cmp(bi)));
    for (index, _) in candidates {
        let mut spans = packet.spans.clone();
        spans.push(scores[index].unit.context.clone());
        let spans = merged(spans);
        let bytes: usize = spans.iter().map(|r| r.end - r.start).sum();
        if bytes <= byte_budget {
            packet.spans = spans;
            packet.delivered_text_bytes = bytes;
            packet.selected_units.push(index);
        } else {
            packet.budget_omitted_units.push(index);
        }
    }
    Ok(packet)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offsets_are_exact_and_context_keeps_neighbors() {
        let text = "# Storage\n\nTemporary entries expire. They cannot be restored.\n\nPersistent entries can be restored.\n";
        for arm in [
            Granularity::Document,
            Granularity::Paragraph,
            Granularity::Sentence,
            Granularity::Window,
        ] {
            for unit in units(text, arm) {
                assert!(text.get(unit.context.clone()).is_some());
                assert!(
                    unit.context.start <= unit.target.start && unit.target.end <= unit.context.end
                );
            }
        }
        let windows = units(text, Granularity::Window);
        assert_eq!(&text[windows[1].context.clone()], text);
    }

    #[test]
    fn fences_tables_and_unicode_remain_whole() {
        let text = "é. No.\n\n```rust\nlet x = 1;\n\n// Keep this. Next.\n```\n\n| Asset | Issuer |\n| A | G1 |\n";
        let values = units(text, Granularity::Sentence);
        assert_eq!(
            values
                .iter()
                .filter(|u| text[u.target.clone()].contains("```rust"))
                .count(),
            1
        );
        assert!(values
            .iter()
            .any(|u| text[u.target.clone()].contains("// Keep this. Next.")));
        assert!(values
            .iter()
            .any(|u| text[u.target.clone()].contains("| Asset | Issuer |\n| A | G1 |")));
        assert_eq!(
            values
                .iter()
                .map(|u| &text[u.target.clone()])
                .collect::<String>(),
            text
        );
    }

    #[test]
    fn pack_merges_overlap_reports_unknown_and_never_truncates() {
        let text = "first second third";
        let make = |target: Range<usize>, context: Range<usize>, probability| UnitScore {
            unit: EvidenceUnit { target, context },
            probability,
        };
        let scores = vec![
            make(0..5, 0..12, Some(0.9)),
            make(6..12, 6..18, Some(0.8)),
            make(13..18, 13..18, None),
            make(0..5, 0..5, Some(0.1)),
        ];
        let packet = pack(text, &scores, 0.4, 18).unwrap();
        assert_eq!(packet.spans, vec![0..18]);
        assert_eq!(packet.delivered_text_bytes, 18);
        assert_eq!(packet.unscored_units, vec![2]);
        assert_eq!(packet.below_threshold_units, vec![3]);
        let small = pack(text, &scores, 0.4, 6).unwrap();
        assert!(small.spans.is_empty());
        assert_eq!(small.budget_omitted_units, vec![0, 1]);
    }

    #[test]
    fn invalid_scores_and_utf8_offsets_are_rejected() {
        let score = UnitScore {
            unit: EvidenceUnit {
                target: 1..2,
                context: 0..2,
            },
            probability: Some(0.8),
        };
        assert!(pack("é", &[score], 0.4, 20).is_err());
        assert!(pack("x", &[], f64::NAN, 20).is_err());
        assert!(pack("x", &[], 0.4, 0).is_err());
    }

    #[test]
    fn loose_lists_and_tables_without_outer_pipes_stay_intact() {
        for text in [
            "+ First item. Preserve it.\n\n+ Second item. Preserve its exception.\n",
            "1) First item. Preserve it.\n\n2) Second item. Preserve its exception.\n",
            "Asset | Issuer\n--- | ---\nUSD | G1. This is one issuer.\n",
        ] {
            let values = units(text, Granularity::Sentence);
            assert_eq!(values.len(), 1, "{text}");
            assert_eq!(&text[values[0].context.clone()], text);
        }
    }

    #[test]
    fn invalid_fence_closer_does_not_expose_code_as_prose() {
        let text = "```text\nExample\n```not_a_close\n\nKeep this. And this.\n```\n";
        let values = units(text, Granularity::Sentence);
        assert_eq!(values.len(), 1);
        assert_eq!(&text[values[0].context.clone()], text);
    }
}
