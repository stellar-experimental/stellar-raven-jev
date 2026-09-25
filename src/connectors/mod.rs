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
/// any window the source advertises before it sends them.
pub fn first_requests<'a>(sources: impl IntoIterator<Item = &'a Source>) -> Vec<(String, u64)> {
    let mut demands = std::collections::BTreeMap::<String, u64>::new();
    for source in sources {
        let scope = match source.family.as_str() {
            "stellarlight" => stellarlight::first_request_scope(source),
            _ => None,
        };
        if let Some(scope) = scope {
            *demands.entry(scope).or_default() += 1;
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

/// The capped hosts that fetching `sources` uses, with their slot counts, in a stable order.
pub fn host_caps<'a>(
    sources: impl IntoIterator<Item = &'a Source>,
    slots: &std::collections::BTreeMap<String, usize>,
) -> Vec<(String, usize)> {
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
