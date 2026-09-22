pub mod algolia;
pub mod lumenloop;
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

pub async fn fetch(ctx: &FetchContext, source: &Source, question: &str) -> Result<FetchResult> {
    match source.family.as_str() {
        "lumenloop" => lumenloop::fetch(ctx, source, question).await,
        "stellarlight" => stellarlight::fetch(ctx, source, question).await,
        "algolia" => algolia::fetch(ctx, source, question).await,
        family => bail!("Unknown connector family: {family}"),
    }
}
