//! Two bounded source reads without constructing a Jev client.
use anyhow::{ensure, Context, Result};
use clap::Parser;
use serde_json::json;
use std::path::PathBuf;
use stellar_raven_jev::{http::HttpRecorder, operations, types::*};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    live: bool,
    #[arg(long, requires = "live")]
    env_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(args.output.is_absolute(), "Output must be absolute");
    ensure!(!args.output.exists(), "Output exists; no resumption");
    if args.live {
        let env_file = args
            .env_file
            .as_ref()
            .context("Live mode needs --env-file")?;
        ensure!(env_file.is_absolute(), "Credential file must be absolute");
        dotenvy::from_path(env_file)
            .map_err(|_| anyhow::anyhow!("Cannot load the explicit credential file"))?;
    }
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(&args.output)?;
    let config = RunConfig {
        fixture: !args.live,
        output_dir: args.output.clone(),
        budget_usd: 0.0,
        max_documents: 1,
        per_source_documents: 1,
        max_pages: 1,
        max_body_bytes: 65_536,
        timeout_secs: 15,
        ..RunConfig::default()
    };
    let http = HttpRecorder::new_bounded(&args.output, &config, 2, 131_072, 30)?;
    let ctx = FetchContext {
        http: http.clone(),
        config,
    };
    let mut rows = Vec::new();
    let mut stopped = false;
    for kind in ["categories", "regions"] {
        let result = operations::fetch(&ctx, "lumenloop.vocabulary", &json!({"kind":kind})).await;
        let row = match result {
            Ok(result) => {
                let complete = result.failures.is_empty() && result.documents.len() == 1;
                std::fs::write(
                    args.output.join(format!("{kind}.json")),
                    serde_json::to_vec_pretty(&result)?,
                )?;
                stopped = !complete;
                json!({"kind":kind,"state":if complete {"complete"} else {"stopped"},"documents":result.documents.len(),"failures":result.failures.len()})
            }
            Err(error) => {
                stopped = true;
                json!({"kind":kind,"state":"stopped","error":error.to_string()})
            }
        };
        rows.push(row);
        std::fs::write(
            args.output.join("summary.json"),
            serde_json::to_vec_pretty(&json!({
                "state":if stopped {"stopped"} else if rows.len()==2 {"complete"} else {"running"},
                "fixture":!args.live,"jev_requests":0,"requested_kinds":["categories","regions"],
                "completion_scope":"bounded reads completed; vocabulary completeness remains unknown",
                "attempted":rows,"http":http.metrics(),"retry_or_resume":false
            }))?,
        )?;
        if stopped {
            break;
        }
    }
    if stopped {
        std::process::exit(2);
    }
    Ok(())
}
