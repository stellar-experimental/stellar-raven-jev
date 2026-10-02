pub mod algolia;
pub mod lumenloop;
pub mod original;
pub mod stellarlight;

use crate::types::*;
use anyhow::{bail, Result};

/// An explicit source scope, separate from the result serialization format.
#[derive(Clone, Copy, Debug, Default, clap::ValueEnum, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceScope {
    #[default]
    All,
    Agentic,
}

impl SourceScope {
    pub fn includes(self, source: &Source) -> bool {
        match self {
            Self::All => true,
            Self::Agentic => matches!(
                source.id.as_str(),
                "algolia:docs:primary"
                    | "stellarlight.skills"
                    | "stellarlight.repos"
                    | "stellarlight.contracts"
                    | "stellarlight.audits"
                    | "stellarlight.research.dev-docs"
                    | "stellarlight.research.repo-docs"
                    | "stellarlight.research.sep"
                    | "stellarlight.research.cap"
                    | "stellarlight.research.release"
                    | "stellarlight.research.audit"
            ),
        }
    }

    pub fn description(self) -> &'static str {
        match self {
            Self::All => "All registered sources can route. Retrieval remains bounded.",
            Self::Agentic => "Developer documentation, standards, repositories, skills, tools, contracts, releases, and audits. Other sources are excluded before routing. This source filter can miss useful articles or research. It does not verify resource safety, freshness, or completeness.",
        }
    }
}

pub fn sources() -> Vec<Source> {
    let mut result = lumenloop::sources();
    result.extend(stellarlight::sources());
    result.extend(algolia::sources());
    result
}

/// The first request of each source, counted per request scope, so a question can book them in
/// any window the source advertises before it sends them. Sources that share one request book it
/// once.
pub fn first_requests<'a>(sources: impl IntoIterator<Item = &'a Source>) -> Vec<(String, u64)> {
    let mut demands = std::collections::BTreeMap::<String, u64>::new();
    for source in sources {
        let scope = match source.family.as_str() {
            "stellarlight" => stellarlight::first_request_scope(source),
            _ => None,
        };
        if let Some(scope) = scope {
            let shared =
                source.family == "stellarlight" && stellarlight::shares_first_request(source);
            let demand = demands.entry(scope).or_default();
            *demand = if shared { 1 } else { *demand + 1 };
        }
    }
    demands.into_iter().collect()
}

/// The host a source's search requests go to, when it is known before fetching.
pub fn search_host(source: &Source) -> Option<String> {
    match source.family.as_str() {
        "stellarlight" => stellarlight::host(),
        "lumenloop" => lumenloop::host(),
        "algolia" => algolia::search_host(source),
        _ => None,
    }
}

/// The in-flight request limit that a source's operator states for one client host, by host.
fn stated_host_limits() -> Vec<(String, usize)> {
    stellarlight::host()
        .map(|host| (host, stellarlight::HOST_CONCURRENCY))
        .into_iter()
        .collect()
}

/// Request windows that source operators state, as (request scope, limit, window length). The
/// governor counts them from the first request, and holds any advertised window of the same scope
/// to the stated limit.
pub fn stated_windows() -> Vec<(String, u64, std::time::Duration)> {
    stellarlight::stated_windows()
}

/// Fetch slots per host: for each host with a stated limit, that limit divided by the requests
/// one question may have in flight on a host (its per-host concurrency, plus hedge permits when
/// hedging is on), and at least 1. Explicit `source_slots` replace these per host and add others.
pub fn source_slots(config: &RunConfig) -> std::collections::BTreeMap<String, usize> {
    let hedges = if config.source_hedge_ms > 0 {
        crate::http::HEDGES_PER_HOST
    } else {
        0
    };
    let per_question = (config.concurrency + hedges).max(1);
    let mut slots: std::collections::BTreeMap<String, usize> = stated_host_limits()
        .into_iter()
        .map(|(host, limit)| (host, (limit / per_question).max(1)))
        .collect();
    slots.extend(config.source_slots.clone());
    slots
}

/// Reject settings that let one question set more requests in flight on a host than its operator
/// states for one client host: fetch slots times the requests one question may have in flight there.
pub fn check_source_slots(config: &RunConfig) -> Result<()> {
    let hedges = if config.source_hedge_ms > 0 {
        crate::http::HEDGES_PER_HOST
    } else {
        0
    };
    let per_question = (config.concurrency + hedges).max(1);
    let slots = source_slots(config);
    for (host, limit) in stated_host_limits() {
        let host_slots = slots.get(&host).copied().unwrap_or(1);
        if host_slots * per_question > limit {
            bail!(
                "{host} states at most {limit} requests in flight for one client host; {host_slots} fetch slots of {per_question} requests each allow {}. Lower --source-slots or --concurrency",
                host_slots * per_question
            );
        }
    }
    Ok(())
}

/// The capped hosts that fetching `sources` uses, with their slot counts, in a stable order.
pub fn host_caps<'a>(
    sources: impl IntoIterator<Item = &'a Source>,
    config: &RunConfig,
) -> Vec<(String, usize)> {
    let slots = source_slots(config);
    let hosts: std::collections::BTreeSet<String> =
        sources.into_iter().filter_map(search_host).collect();
    hosts
        .into_iter()
        .filter_map(|host| slots.get(&host).map(|n| (host, *n)))
        .collect()
}

pub async fn fetch(ctx: &FetchContext, source: &Source, question: &str) -> Result<FetchResult> {
    match source.family.as_str() {
        "lumenloop" => lumenloop::fetch(ctx, source, question).await,
        "stellarlight" => stellarlight::fetch(ctx, source, question).await,
        "algolia" => algolia::fetch(ctx, source, question).await,
        family => bail!("Unknown connector family: {family}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stated_limits_give_default_slots_and_explicit_slots_replace_or_add() {
        let scout = stellarlight::host().unwrap();
        let limit = stellarlight::HOST_CONCURRENCY;
        let mut config = RunConfig::default();
        assert_eq!(source_slots(&config)[&scout], limit / config.concurrency);
        config.concurrency = limit * 2;
        assert_eq!(source_slots(&config)[&scout], 1);
        config.concurrency = 8;
        config.source_hedge_ms = 4_000;
        assert_eq!(
            source_slots(&config)[&scout],
            limit / (8 + crate::http::HEDGES_PER_HOST)
        );
        config.source_slots = std::collections::BTreeMap::from([
            (scout.clone(), 1_000),
            ("fernlet.test".to_owned(), 3),
        ]);
        let slots = source_slots(&config);
        assert_eq!(slots[&scout], 1_000);
        assert_eq!(slots["fernlet.test"], 3);
    }

    #[test]
    fn settings_above_a_stated_host_limit_are_rejected() {
        let scout = stellarlight::host().unwrap();
        let limit = stellarlight::HOST_CONCURRENCY;
        let mut config = RunConfig::default();
        assert!(check_source_slots(&config).is_ok());
        config.source_slots = std::collections::BTreeMap::from([(scout.clone(), 3)]);
        let error = check_source_slots(&config).unwrap_err().to_string();
        assert!(error.contains(&format!("at most {limit}")), "{error}");
        config.source_slots.clear();
        config.concurrency = limit + 1;
        assert!(check_source_slots(&config).is_err());
        config.concurrency = 8;
        config.source_slots = std::collections::BTreeMap::from([("fernlet.test".to_owned(), 40)]);
        assert!(
            check_source_slots(&config).is_ok(),
            "hosts without a stated limit are not checked"
        );
    }
}
