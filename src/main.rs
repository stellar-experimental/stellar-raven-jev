use anyhow::{ensure, Context, Result};
use clap::{Parser, Subcommand};
use serde_json::json;
use std::{
    io::{self, Write},
    path::PathBuf,
};
use stellar_raven_jev::{
    connectors,
    http::HttpRecorder,
    jev::JevClient,
    pipeline::{run_question, validate_config, RunOutcome},
    types::RunConfig,
};

#[derive(Parser)]
#[command(
    version,
    about = "Retrieve source documents with Jev and save complete run evidence. No answer generation."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
    #[arg(
        long,
        global = true,
        help = "Use deterministic offline fixtures. This does not test live Jev."
    )]
    fixture: bool,
    /// Load an explicit absolute credential file. Also accepts JEV_ENV_FILE.
    #[arg(long, global = true)]
    env_file: Option<PathBuf>,
    /// Parent directory for run evidence. Also accepts JEV_OUTPUT_DIR.
    #[arg(long, global = true, env = "JEV_OUTPUT_DIR", default_value = "runs")]
    output_dir: PathBuf,
    /// Maximum Jev allocation for one question. Also accepts JEV_BUDGET_USD.
    #[arg(long, global = true, env = "JEV_BUDGET_USD", default_value_t = 0.0)]
    budget_usd: f64,
    #[arg(long, global = true, default_value_t = 30)]
    timeout_secs: u64,
    #[arg(long, global = true, default_value_t = 16)]
    concurrency: usize,
    #[arg(long, global = true, default_value_t = 2)]
    max_pages: usize,
    #[arg(long, global = true, default_value_t = 100)]
    max_documents: usize,
    #[arg(long, global = true, default_value_t = 12)]
    per_source_documents: usize,
    /// Wall-clock limit for the whole retrieval stage. Unfinished connectors are recorded and dropped.
    #[arg(long, global = true, default_value_t = 10)]
    fetch_deadline_secs: u64,
    #[arg(long, global = true, default_value_t = 8388608)]
    max_body_bytes: usize,
    #[arg(long, global = true, default_value_t = 2)]
    route_passes: usize,
    #[arg(long, global = true, default_value_t = 0.2)]
    source_threshold: f64,
    #[arg(long, global = true, default_value_t = 0.4)]
    document_threshold: f64,
    #[arg(long, global = true, default_value_t = 0.15)]
    uncertain_threshold: f64,
}

#[derive(Subcommand)]
enum Command {
    /// List every registered source family and its retrieval scope.
    Sources {
        #[arg(long, value_enum, default_value = "all")]
        resources: connectors::SourceScope,
    },
    /// Look up Stellar sources and print titles, links, excerpts, and saved text paths.
    Search {
        question: String,
        /// Select source families before routing. Agentic excludes general ecosystem content.
        #[arg(long, value_enum, default_value = "all")]
        resources: connectors::SourceScope,
        /// Print one JSON object containing every selected and uncertain result.
        #[arg(long)]
        json: bool,
        /// Include full available text in output. Original sources may contain only summaries.
        #[arg(long)]
        full_text: bool,
        /// Maximum displayed text results. Zero shows all. JSON always includes all results.
        #[arg(long, default_value_t = 10)]
        limit: usize,
        /// Print a small JSON projection for agents: selected results only, one per URL,
        /// bounded by --limit, with report counts. The saved search.json stays complete.
        #[arg(long)]
        compact: bool,
    },
    /// List typed operation schemas and retrieval capabilities.
    Operations,
    /// Execute an agent-authored retrieval plan against its original question.
    Plan {
        file: PathBuf,
        #[arg(long)]
        dry_run: bool,
    },
    /// Retrieve documents for one question and save an evidence directory.
    Ask { question: String },
    /// Read one question per line. Each question creates a separate evidence directory.
    Chat,
    /// Check local settings and authentication presence without network requests.
    Doctor,
    /// Serve local MCP over stdio. The budget applies to the complete server session.
    Mcp,
}

fn report(outcome: &RunOutcome) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(outcome)?);
    Ok(())
}

fn source_credentials(present: impl Fn(&str) -> bool) -> serde_json::Value {
    let mut sources = serde_json::Map::new();
    for (source, keys) in [
        ("lumenloop", &["LUMENLOOP_API_KEY"][..]),
        (
            "algolia_docs",
            &["ALGOLIA_APPLICATION_ID_DOCS", "ALGOLIA_API_KEY_DOCS"][..],
        ),
        (
            "algolia_site",
            &["ALGOLIA_APPLICATION_ID_SITE", "ALGOLIA_API_KEY_SITE"][..],
        ),
        ("stellarlight", &[][..]),
    ] {
        let presence: std::collections::BTreeMap<_, _> =
            keys.iter().map(|key| (*key, present(key))).collect();
        let missing: Vec<_> = presence
            .iter()
            .filter_map(|(key, value)| (!value).then_some(*key))
            .collect();
        sources.insert(
            source.into(),
            json!({
                "credentials_required":!keys.is_empty(), "present":presence,
                "missing":missing, "ready":missing.is_empty(),
            }),
        );
    }
    serde_json::Value::Object(sources)
}

fn doctor_config(config: &RunConfig, directory: PathBuf) -> RunConfig {
    let mut result = config.clone();
    result.output_dir = directory;
    if result.budget_usd == 0.0 {
        // Construction only: this temporary allowance is never used for a request.
        result.budget_usd = 1.0;
    }
    result
}

fn wrangler_on_path() -> bool {
    std::env::var_os("PATH").is_some_and(|path| {
        std::env::split_paths(&path).any(|directory| {
            let executable = directory.join("wrangler");
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                executable.metadata().is_ok_and(|metadata| {
                    metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
                })
            }
            #[cfg(not(unix))]
            {
                executable.is_file()
            }
        })
    })
}

fn profile_doctor_check(
    config: &RunConfig,
    env: impl Fn(&str) -> Option<String>,
    wrangler_present: impl FnOnce() -> bool,
) -> Result<Option<serde_json::Value>> {
    if config.fixture {
        return Ok(None);
    }
    let backend = env("JEV_BACKEND").unwrap_or_else(|| {
        if env("JEV_PROXY_URL").is_some() {
            "proxy"
        } else if env("TYPESAFE_API_KEY").is_some() {
            "typesafe"
        } else {
            "cloudflare"
        }
        .into()
    });
    if backend != "cloudflare" || env("CLOUDFLARE_API_TOKEN").is_some() {
        return Ok(None);
    }
    let Some(profile) = env("JEV_CLOUDFLARE_AUTH_PROFILE") else {
        return Ok(None);
    };
    let account =
        env("CLOUDFLARE_ACCOUNT_ID").context("Missing Jev configuration: CLOUDFLARE_ACCOUNT_ID")?;
    ensure!(
        account.len() == 32 && account.bytes().all(|b| b.is_ascii_hexdigit()),
        "CLOUDFLARE_ACCOUNT_ID must contain 32 hexadecimal characters"
    );
    ensure!(
        !profile.is_empty()
            && profile.len() <= 128
            && profile
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            && !profile.starts_with('-'),
        "JEV_CLOUDFLARE_AUTH_PROFILE has an invalid format"
    );
    ensure!(
        cfg!(unix),
        "Wrangler profile authentication requires Unix; configure CLOUDFLARE_API_TOKEN"
    );
    ensure!(
        wrangler_present(),
        "Wrangler executable is missing from PATH"
    );
    Ok(Some(json!({
        "mode":"wrangler_profile_local_only", "wrangler_present":true,
        "profile_token_resolved":false, "profile_login_validated":false,
        "authorization_validated":false, "oauth_refresh_deferred_until_actual_run":true,
    })))
}

fn doctor_jev_inspection(
    config: &RunConfig,
    directory: PathBuf,
    env: impl Fn(&str) -> Option<String>,
    wrangler_present: impl FnOnce() -> bool,
    resolve: impl FnOnce(&RunConfig, &HttpRecorder) -> Result<()>,
) -> Result<serde_json::Value> {
    if let Some(check) = profile_doctor_check(config, env, wrangler_present)? {
        return Ok(check);
    }
    let inspection_config = doctor_config(config, directory.clone());
    let http = HttpRecorder::new(&directory, &inspection_config)?;
    resolve(&inspection_config, &http)?;
    Ok(json!({"mode":if config.fixture {"fixture"} else {"configured_credentials"}}))
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if let Some(path) = cli
        .env_file
        .clone()
        .or_else(|| std::env::var_os("JEV_ENV_FILE").map(PathBuf::from))
    {
        ensure!(
            path.is_absolute(),
            "The explicit credential file must use an absolute path"
        );
        dotenvy::from_path(path)
            .map_err(|_| anyhow::anyhow!("Cannot load the explicit JEV_ENV_FILE or --env-file"))?;
    } else if matches!(cli.command, Command::Mcp) {
        // An MCP host can start in another project's working directory.
        // Load credentials only from its explicit trusted configuration.
    } else {
        dotenvy::dotenv().ok();
    }
    let config = RunConfig {
        fixture: cli.fixture,
        output_dir: cli.output_dir,
        budget_usd: cli.budget_usd,
        timeout_secs: cli.timeout_secs,
        concurrency: cli.concurrency,
        max_pages: cli.max_pages,
        max_documents: cli.max_documents,
        per_source_documents: cli.per_source_documents,
        fetch_deadline_secs: cli.fetch_deadline_secs,
        max_body_bytes: cli.max_body_bytes,
        route_passes: cli.route_passes,
        source_threshold: cli.source_threshold,
        document_threshold: cli.document_threshold,
        uncertain_threshold: cli.uncertain_threshold,
    };
    validate_config(&config)?;
    match cli.command {
        Command::Sources { resources } => println!(
            "{}",
            serde_json::to_string_pretty(
                &connectors::sources()
                    .into_iter()
                    .filter(|s| resources.includes(s))
                    .collect::<Vec<_>>()
            )?
        ),
        Command::Search {
            question,
            resources,
            json,
            full_text,
            limit,
            compact,
        } => {
            eprintln!(
                "Retrieving and scoring sources. Full evidence will remain in the run directory."
            );
            let outcome =
                stellar_raven_jev::pipeline::run_question_scoped(&question, &config, resources)
                    .await?;
            let report = stellar_raven_jev::search::build_report(&outcome, full_text)?;
            if compact {
                let projection = stellar_raven_jev::search::compact_report(&report, limit);
                println!("{}", serde_json::to_string(&projection)?);
            } else if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                print!("{}", stellar_raven_jev::search::render_text(&report, limit));
            }
            if outcome.status != "complete" {
                std::process::exit(if outcome.status == "partial" { 2 } else { 1 });
            }
        }
        Command::Operations => println!(
            "{}",
            serde_json::to_string_pretty(&stellar_raven_jev::operations::catalog())?
        ),
        Command::Plan { file, dry_run } => {
            use std::io::Read;
            let mut bytes = Vec::new();
            std::fs::File::open(file)?
                .take(256 * 1024 + 1)
                .read_to_end(&mut bytes)?;
            ensure!(bytes.len() <= 256 * 1024, "Plan file exceeds 256 KiB");
            let plan: stellar_raven_jev::plan::RetrievalPlan = serde_json::from_slice(&bytes)?;
            stellar_raven_jev::plan::validate(&plan)?;
            if dry_run {
                println!(
                    "{}",
                    json!({"valid":true,"network_called":false,"plan":plan})
                );
            } else {
                let outcome = stellar_raven_jev::plan::run_plan(&plan, &config).await?;
                report(&outcome)?;
                if outcome.status != "complete" {
                    std::process::exit(if outcome.status == "partial" { 2 } else { 1 });
                }
            }
        }
        Command::Mcp => stellar_raven_jev::mcp::serve(config).await?,
        Command::Ask { question } => {
            let outcome = run_question(&question, &config).await?;
            report(&outcome)?;
            if outcome.status != "complete" {
                std::process::exit(1);
            }
        }
        Command::Chat => {
            eprintln!("Enter one question per line. Enter /quit to stop. Each question uses the configured per-run budget.");
            let mut line = String::new();
            loop {
                eprint!("question> ");
                io::stderr().flush()?;
                line.clear();
                if io::stdin().read_line(&mut line)? == 0 {
                    break;
                }
                let question = line.trim();
                if matches!(question, "/quit" | "/exit") {
                    break;
                }
                if question.is_empty() {
                    continue;
                }
                match run_question(question, &config).await {
                    Ok(outcome) => report(&outcome)?,
                    Err(error) => eprintln!("Run failed: {error}"),
                }
            }
        }
        Command::Doctor => {
            let directory =
                std::env::temp_dir().join(format!("raven-doctor-{}", uuid::Uuid::new_v4()));
            let result = doctor_jev_inspection(
                &config,
                directory.clone(),
                |key| {
                    std::env::var(key)
                        .ok()
                        .filter(|value| !value.trim().is_empty())
                },
                wrangler_on_path,
                |config, http| JevClient::new(config, http).map(|_| ()),
            );
            let _ = std::fs::remove_dir_all(&directory);
            let source_credentials = source_credentials(|key| {
                std::env::var(key).is_ok_and(|value| !value.trim().is_empty())
            });
            let source_credentials_ready = source_credentials
                .as_object()
                .unwrap()
                .values()
                .all(|source| source["ready"] == true);
            let configuration_ready =
                result.is_ok() && (config.fixture || source_credentials_ready);
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "mode":if config.fixture {"offline-fixture"} else {"live-jev"},
                    "network_checked":false,"authentication_validated_remotely":false,
                    "oauth_refresh_performed":false,
                    "jev_authentication_check":result.as_ref().ok(),
                    "local_configuration_ready":configuration_ready,
                    "jev_configuration_ready":result.is_ok(),
                    "source_credentials":source_credentials,
                    "source_credentials_ready":source_credentials_ready,
                    "error":result.as_ref().err().map(ToString::to_string),
                    "source_count":connectors::sources().len(),"budget_usd":config.budget_usd,
                    "live_run_budget_ready":config.budget_usd > 0.0 && config.budget_usd <= 100.0,
                    "budget_readiness_scope":"Positive allocation within the shared ceiling. Jev checks each request reservation before spending.",
                    "note":"This check makes no network or paid requests. Configuration checks do not require a spending allocation."
                }))?
            );
            if !configuration_ready {
                std::process::exit(1);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile_env(key: &str) -> Option<String> {
        match key {
            "JEV_BACKEND" => Some("cloudflare".into()),
            "CLOUDFLARE_ACCOUNT_ID" => Some("00000000000000000000000000000000".into()),
            "JEV_CLOUDFLARE_AUTH_PROFILE" => Some("personal".into()),
            _ => None,
        }
    }

    #[test]
    fn doctor_profile_branch_never_calls_the_authentication_resolver() {
        let dir = tempfile::tempdir().unwrap();
        let check = doctor_jev_inspection(
            &RunConfig::default(),
            dir.path().join("unused"),
            profile_env,
            || true,
            |_, _| panic!("Doctor must not resolve or refresh OAuth"),
        )
        .unwrap();
        assert_eq!(check["oauth_refresh_deferred_until_actual_run"], true);
        assert_eq!(check["authorization_validated"], false);
        assert!(!dir.path().join("unused").exists());
    }

    #[test]
    fn doctor_profile_checks_local_prerequisites_without_resolving_a_token() {
        let check = profile_doctor_check(&RunConfig::default(), profile_env, || true)
            .unwrap()
            .unwrap();
        assert_eq!(check["mode"], "wrangler_profile_local_only");
        assert_eq!(check["profile_token_resolved"], false);
        assert_eq!(check["profile_login_validated"], false);
        assert!(profile_doctor_check(&RunConfig::default(), profile_env, || false).is_err());
        assert!(profile_doctor_check(
            &RunConfig::default(),
            |key| {
                if key == "JEV_CLOUDFLARE_AUTH_PROFILE" {
                    Some("--bad".into())
                } else {
                    profile_env(key)
                }
            },
            || panic!("Invalid configuration must fail before checking Wrangler")
        )
        .is_err());
    }

    #[test]
    fn doctor_static_token_and_fixture_do_not_check_wrangler() {
        assert!(profile_doctor_check(
            &RunConfig::default(),
            |key| {
                if key == "CLOUDFLARE_API_TOKEN" {
                    Some("test-placeholder".into())
                } else {
                    profile_env(key)
                }
            },
            || panic!("A static token takes precedence")
        )
        .unwrap()
        .is_none());
        let config = RunConfig {
            fixture: true,
            ..Default::default()
        };
        assert!(
            profile_doctor_check(&config, profile_env, || panic!("Fixture mode is offline"))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn doctor_checks_credentials_without_changing_the_requested_budget() {
        let config = RunConfig::default();
        let inspection = doctor_config(&config, PathBuf::from("temporary-doctor"));
        assert_eq!(config.budget_usd, 0.0);
        assert!(inspection.budget_usd > 0.0);
        assert_eq!(inspection.output_dir, PathBuf::from("temporary-doctor"));
        let allocated = RunConfig {
            budget_usd: 2.0,
            ..Default::default()
        };
        assert_eq!(doctor_config(&allocated, PathBuf::new()).budget_usd, 2.0);
    }

    #[test]
    fn doctor_reports_source_presence_and_missing_keys_without_values() {
        let credentials = source_credentials(|key| {
            matches!(key, "LUMENLOOP_API_KEY" | "ALGOLIA_APPLICATION_ID_DOCS")
        });
        assert_eq!(credentials["lumenloop"]["ready"], true);
        assert_eq!(credentials["algolia_docs"]["ready"], false);
        assert_eq!(
            credentials["algolia_docs"]["missing"],
            json!(["ALGOLIA_API_KEY_DOCS"])
        );
        assert_eq!(credentials["stellarlight"]["credentials_required"], false);
        assert_eq!(credentials["stellarlight"]["ready"], true);
        for source in credentials.as_object().unwrap().values() {
            assert!(source["present"]
                .as_object()
                .unwrap()
                .values()
                .all(serde_json::Value::is_boolean));
        }
    }
}
