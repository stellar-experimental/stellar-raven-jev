use anyhow::{ensure, Context, Result};
use clap::{Parser, Subcommand};
use serde_json::json;
use std::path::PathBuf;
use stellar_raven_jev::{
    connectors,
    http::HttpRecorder,
    jev::JevClient,
    pipeline::{validate_config, RunOutcome},
    types::RunConfig,
};

#[derive(Parser)]
#[command(
    version,
    about = "Retrieve and rank Stellar source documents with Jev for agents. No answer generation."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
    /// Use deterministic offline fixtures. This does not test live Jev.
    #[arg(long, global = true, hide = true)]
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
    #[arg(long, global = true, hide = true, default_value_t = 30)]
    timeout_secs: u64,
    #[arg(long, global = true, hide = true, default_value_t = 16)]
    concurrency: usize,
    #[arg(long, global = true, hide = true, default_value_t = 32)]
    jev_concurrency: usize,
    #[arg(long, global = true, hide = true, default_value_t = 2000)]
    jev_hedge_ms: u64,
    /// Chunks per scoring call. One per call is the default: in a replay, chunks that shared a
    /// call changed each other's scores (91 of 1,655 documents crossed the selection threshold,
    /// against 18 between two single-chunk runs).
    #[arg(long, global = true, hide = true, default_value_t = 1)]
    jev_batch: usize,
    /// Reference date (YYYY-MM-DD) for currentness judgments. Defaults to today in UTC.
    #[arg(long, global = true, hide = true)]
    today: Option<String>,
    #[arg(long, global = true, hide = true, default_value_t = 2)]
    max_pages: usize,
    /// Scoring admission limit across all sources. It is an operating budget, set high enough that
    /// every fetched document is normally scored.
    #[arg(long, global = true, hide = true, default_value_t = 400)]
    max_documents: usize,
    #[arg(long, global = true, hide = true, default_value_t = 12)]
    per_source_documents: usize,
    /// Wall-clock limit for the whole retrieval stage. Unfinished connectors are recorded and dropped.
    #[arg(long, global = true, hide = true, default_value_t = 10)]
    fetch_deadline_secs: u64,
    #[arg(long, global = true, hide = true, default_value_t = 8388608)]
    max_body_bytes: usize,
    #[arg(long, global = true, hide = true, default_value_t = 2)]
    route_passes: usize,
    #[arg(long, global = true, hide = true, default_value_t = 0.2)]
    source_threshold: f64,
    /// Sources routed below this stay in the session as pools. The default, equal to the source
    /// threshold, fetches every routed source; 0.4 is the lean first pass.
    #[arg(long, global = true, hide = true, default_value_t = 0.2)]
    fetch_threshold: f64,
    /// Documents scored per source before the rest waits for promise. The default 0 scores
    /// everything; 4 is the lean first pass.
    #[arg(long, global = true, hide = true, default_value_t = 0)]
    score_depth: usize,
    /// Most original pages one call reads for listing rows (at most 4). 0 reads none.
    #[arg(long, global = true, hide = true, default_value_t = 4)]
    original_reads: usize,
    #[arg(long, global = true, hide = true, default_value_t = 0.4)]
    document_threshold: f64,
    #[arg(long, global = true, hide = true, default_value_t = 0.15)]
    uncertain_threshold: f64,
    /// Searches that may run at once on this host, across processes that share the output
    /// directory. Also accepts JEV_MAX_SEARCHES.
    #[arg(
        long,
        global = true,
        hide = true,
        env = "JEV_MAX_SEARCHES",
        default_value_t = 6
    )]
    max_searches: usize,
    /// Folder of host-wide state (admission slots, Jev budgets, source rate limits). Every search
    /// that should share this host's capacity uses the same folder. Defaults to
    /// OUTPUT_DIR/.host. Also accepts JEV_HOST_DIR.
    #[arg(long, global = true, env = "JEV_HOST_DIR")]
    host_dir: Option<PathBuf>,
    /// Most questions that may fetch from a source host at once on this host, as
    /// `HOST=N[,HOST=N]` (for example `stellarlight.xyz=3`). A question waits up to about a
    /// minute for a slot on every capped host it fetches from, then reports `busy`. Hosts not
    /// named are not capped. Also accepts JEV_SOURCE_SLOTS.
    #[arg(long, global = true, env = "JEV_SOURCE_SLOTS", value_parser = parse_source_slots)]
    source_slots: Option<std::collections::BTreeMap<String, usize>>,
    /// How long a search waits for a free slot before it reports `busy`.
    #[arg(long, global = true, hide = true, default_value_t = 60)]
    admission_wait_secs: u64,
}

#[derive(Subcommand)]
enum Command {
    /// List every registered source family and its retrieval scope.
    Sources {
        #[arg(long, value_enum, default_value = "all")]
        resources: connectors::SourceScope,
    },
    /// Look up Stellar sources. Prints compact JSON: ranked results with URLs, excerpts, and text paths.
    Search {
        question: String,
        /// Select source families before routing. Agentic excludes general ecosystem content.
        #[arg(long, value_enum, default_value = "all")]
        resources: connectors::SourceScope,
        /// Print the full ranked report, with every selected and uncertain result.
        #[arg(long)]
        json: bool,
        /// Include full available text in output. Original sources may contain only summaries.
        #[arg(long)]
        full_text: bool,
        /// Results in the compact output, one per URL. Zero shows all selected results.
        #[arg(long, default_value_t = 10)]
        limit: usize,
        /// Save the full audit record for evaluation and replay: raw HTTP bodies, Jev traces,
        /// every scored document, and routing. By default only the report and its text files stay.
        #[arg(long)]
        full_record: bool,
    },
    /// Rebuild the report from a run saved with --full-record, without retrieval or scoring.
    Report {
        directory: PathBuf,
        /// Name for the replayed output files, so the original search.json stays unchanged.
        #[arg(long, default_value = "replay")]
        variant: String,
        /// Print the full ranked report instead of the compact output.
        #[arg(long)]
        json: bool,
        /// Include full available text in output.
        #[arg(long)]
        full_text: bool,
        /// Results in the compact output, one per URL. Zero shows all selected results.
        #[arg(long, default_value_t = 10)]
        limit: usize,
    },
    /// Continue a session: spend open pools (score a source's unscored documents, or fetch a
    /// source that was routed but not fetched), then print the re-ranked session.
    More {
        /// The session folder (`session.id` of an earlier call).
        session: PathBuf,
        /// Pool IDs from the `pools` list, comma separated.
        #[arg(long, value_delimiter = ',', required_unless_present = "all")]
        pool: Vec<String>,
        /// Spend every open pool.
        #[arg(long, conflicts_with = "pool")]
        all: bool,
        /// Print the full ranked report.
        #[arg(long)]
        json: bool,
        /// Include full available text in output.
        #[arg(long)]
        full_text: bool,
        /// Results in the compact output, one per URL. Zero shows all selected results.
        #[arg(long, default_value_t = 10)]
        limit: usize,
    },
    /// Ask Jev which session documents support, contradict, or qualify each claim you give.
    Check {
        /// The session folder (`session.id` of an earlier call).
        session: PathBuf,
        /// One to four claims, each a short statement to test against the documents.
        #[arg(required = true, num_args = 1..=4)]
        claims: Vec<String>,
        /// Which documents to read: selected (selected and uncertain), scored, or all.
        #[arg(long, value_enum, default_value = "selected")]
        scope: stellar_raven_jev::session::CheckScope,
        /// Rows per list.
        #[arg(long, default_value_t = 5)]
        limit: usize,
    },
    /// Check local settings and authentication presence without network requests.
    Doctor,
}

/// The question a session was started with.
fn session_question(session: &std::path::Path) -> Result<String> {
    let record: serde_json::Value = serde_json::from_slice(
        &std::fs::read(session.join("question.json"))
            .with_context(|| format!("{} is not a session folder", session.display()))?,
    )?;
    record["question"]
        .as_str()
        .map(str::to_owned)
        .context("The session lacks its question")
}

/// Hold the session for this call, or print `busy` and exit 3 when another call holds it.
fn hold_session(session: &std::path::Path, question: &str) -> Result<std::fs::File> {
    match stellar_raven_jev::session::lock(session)? {
        Some(file) => Ok(file),
        None => {
            println!(
                "{}",
                json!({"schema_version":1,"compact":true,"question":question,"status":"busy",
                    "retry_after_ms":5_000,"session":{"id":session},
                    "message":"Another call is using this session. Nothing was spent. Retry when it finishes."})
            );
            std::process::exit(3);
        }
    }
}

/// Whether a session keeps its full audit record.
fn session_full_record(session: &std::path::Path) -> Result<bool> {
    let record: serde_json::Value =
        serde_json::from_slice(&std::fs::read(session.join("question.json"))?)?;
    Ok(record["config"]["full_record"] == true)
}

/// `HOST=N[,HOST=N]`, each N at least 1, each host named once.
fn parse_source_slots(text: &str) -> Result<std::collections::BTreeMap<String, usize>, String> {
    let mut slots = std::collections::BTreeMap::new();
    for part in text.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (host, count) = part
            .split_once('=')
            .ok_or_else(|| format!("`{part}` is not HOST=N"))?;
        let host = host.trim().to_ascii_lowercase();
        let count: usize = count
            .trim()
            .parse()
            .map_err(|_| format!("`{part}`: N must be a whole number"))?;
        if host.is_empty() || count == 0 {
            return Err(format!(
                "`{part}`: the host must be named and N must be at least 1"
            ));
        }
        if slots.insert(host, count).is_some() {
            return Err(format!("`{part}`: the host is named twice"));
        }
    }
    Ok(slots)
}

/// Take one of this host's search slots, or print `busy` and exit 3 without spending. Calls that
/// share an output directory share one host state folder: admission slots, Jev provider budgets
/// and cooldowns, and source rate-limit gates.
async fn admit(
    slots: (usize, u64, Option<PathBuf>),
    config: &mut RunConfig,
    question: &str,
) -> Result<stellar_raven_jev::governor::Admission> {
    let (max_searches, admission_wait_secs, host_dir) = slots;
    let host_dir = host_dir.unwrap_or_else(|| config.output_dir.join(".host"));
    let governor = stellar_raven_jev::governor::Governor::at(&host_dir)?;
    let Some(admission) = governor
        .admit(
            max_searches,
            std::time::Duration::from_secs(admission_wait_secs),
        )
        .await?
    else {
        println!(
            "{}",
            json!({"schema_version":1,"compact":true,"question":question,"status":"busy",
                "retry_after_ms":BUSY_RETRY_AFTER_MS,"load":{"admission_wait_ms":admission_wait_secs * 1000,"max_searches":max_searches},
                "message":"This host is already running its maximum number of searches. Nothing was spent. Retry later."})
        );
        std::process::exit(3);
    };
    config.host_dir = Some(host_dir);
    Ok(admission)
}

/// Print a run or session report, reduce a light record, and exit with the run's status code.
fn deliver(
    outcome: &RunOutcome,
    question: &str,
    admission: stellar_raven_jev::governor::Admission,
    json: bool,
    full_text: bool,
    limit: usize,
    full_record: bool,
) -> Result<()> {
    if outcome.status == "busy" {
        drop(admission);
        // The question found no room in a source's request window.
        println!(
            "{}",
            json!({"schema_version":1,"compact":true,"question":question,"status":"busy",
                "retry_after_ms":outcome.retry_after_ms.unwrap_or(BUSY_RETRY_AFTER_MS),
                "usage":outcome.usage,
                "session":{"id":outcome.directory},
                "message":"A source this question needs has no request capacity left now. Nothing more was spent. Retry after retry_after_ms."})
        );
        if !full_record {
            let _ = stellar_raven_jev::pipeline::keep_session_record(&outcome.directory);
        }
        std::process::exit(3);
    }
    let mut report = stellar_raven_jev::search::build_report_variant(outcome, full_text, None)?;
    report["load"]["admission_wait_ms"] = json!(admission.waited_ms);
    drop(admission);
    print_report(&report, json, limit)?;
    // Clean up after printing, so a cleanup error never loses a paid result.
    if !full_record {
        if let Err(error) = stellar_raven_jev::pipeline::keep_session_record(&outcome.directory) {
            eprintln!("Run folder cleanup failed; intermediate files remain: {error}");
        }
    }
    if outcome.status != "complete" {
        std::process::exit(if outcome.status == "partial" { 2 } else { 1 });
    }
    Ok(())
}

/// A refused search is told to retry after about this long.
const BUSY_RETRY_AFTER_MS: u64 = 10_000;

/// Compact JSON by default; the full ranked report with --json.
fn print_report(report: &serde_json::Value, json: bool, limit: usize) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(report)?);
    } else {
        let projection = stellar_raven_jev::search::compact_report(report, limit);
        println!("{}", serde_json::to_string(&projection)?);
    }
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
    // Scout works without a key; a partner key raises its request limits.
    sources["stellarlight"]["partner_key_present"] = json!(present("STELLAR_LIGHT_API_KEY"));
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
    // The chain must be valid, and only a chain with Cloudflare needs the Wrangler check.
    let order = stellar_raven_jev::jev::provider_order(&env)?;
    if !order.iter().any(|name| name == "cloudflare") {
        return Ok(None);
    }
    if env("CLOUDFLARE_API_TOKEN").is_some() {
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
    } else {
        dotenvy::dotenv().ok();
    }
    let mut config = RunConfig {
        fixture: cli.fixture,
        output_dir: cli.output_dir.clone(),
        budget_usd: cli.budget_usd,
        timeout_secs: cli.timeout_secs,
        concurrency: cli.concurrency,
        jev_concurrency: cli.jev_concurrency,
        jev_hedge_ms: cli.jev_hedge_ms,
        jev_batch: cli.jev_batch,
        today: cli
            .today
            .clone()
            .unwrap_or_else(stellar_raven_jev::rank::today_utc),
        max_pages: cli.max_pages,
        max_documents: cli.max_documents,
        per_source_documents: cli.per_source_documents,
        fetch_deadline_secs: cli.fetch_deadline_secs,
        max_body_bytes: cli.max_body_bytes,
        route_passes: cli.route_passes,
        source_threshold: cli.source_threshold,
        fetch_threshold: cli.fetch_threshold,
        score_depth: cli.score_depth,
        document_threshold: cli.document_threshold,
        uncertain_threshold: cli.uncertain_threshold,
        full_record: true,
        host_dir: None,
        source_slots: cli.source_slots.clone().unwrap_or_default(),
        original_reads: cli.original_reads,
    };
    validate_config(&config)?;
    let slots = (
        cli.max_searches,
        cli.admission_wait_secs,
        cli.host_dir.clone(),
    );
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
            full_record,
        } => {
            config.full_record = full_record;
            let admission = admit(slots.clone(), &mut config, &question).await?;
            let outcome =
                stellar_raven_jev::pipeline::run_question_scoped(&question, &config, resources)
                    .await?;
            stellar_raven_jev::session::record_call(
                &outcome.directory,
                "search",
                json!({"question":question,"resources":format!("{resources:?}").to_lowercase()}),
                &outcome.usage,
            )?;
            stellar_raven_jev::session::set_budget_cap(&outcome.directory, config.budget_usd)?;
            deliver(
                &outcome,
                &question,
                admission,
                json,
                full_text,
                limit,
                full_record,
            )?;
        }
        Command::More {
            session,
            pool,
            all,
            json,
            full_text,
            limit,
        } => {
            let question = session_question(&session)?;
            let _held = hold_session(&session, &question)?;
            let admission = admit(slots.clone(), &mut config, &question).await?;
            let outcome = stellar_raven_jev::pipeline::continue_session(
                &session,
                (!all).then_some(pool.as_slice()),
                &config,
            )
            .await?;
            if outcome.status != "busy" {
                stellar_raven_jev::session::record_call(
                    &outcome.directory,
                    "more",
                    json!({"pools": if all { json!("all") } else { json!(pool) }}),
                    &outcome.usage,
                )?;
            }
            let full_record = session_full_record(&session)?;
            deliver(
                &outcome,
                &question,
                admission,
                json,
                full_text,
                limit,
                full_record,
            )?;
        }
        Command::Check {
            session,
            claims,
            scope,
            limit,
        } => {
            let question = session_question(&session)?;
            let _held = hold_session(&session, &question)?;
            let admission = admit(slots.clone(), &mut config, &question).await?;
            let result =
                stellar_raven_jev::session::check(&session, &claims, scope, limit, &config).await?;
            drop(admission);
            println!("{}", serde_json::to_string(&result)?);
        }
        Command::Report {
            directory,
            variant,
            json,
            full_text,
            limit,
        } => {
            anyhow::ensure!(
                directory.join("documents.json").is_file(),
                "report needs a run folder with its session state; {} has none",
                directory.display()
            );
            let manifest: serde_json::Value =
                serde_json::from_slice(&std::fs::read(directory.join("manifest.json"))?)?;
            let mut outcome: RunOutcome = serde_json::from_value(manifest["outcome"].clone())
                .context("manifest.json lacks a run outcome")?;
            outcome.directory = directory;
            let report = stellar_raven_jev::search::build_report_variant(
                &outcome,
                full_text,
                Some(&variant),
            )?;
            print_report(&report, json, limit)?;
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
                    "jev_providers":stellar_raven_jev::jev::provider_order(&|key: &str| {
                        std::env::var(key).ok().filter(|value| !value.trim().is_empty())
                    })
                    .map_or_else(|error| json!({"error": error.to_string()}), |order| json!(order)),
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

    #[test]
    fn source_slots_parse_strictly() {
        let slots = parse_source_slots("cedar.test=3, Birch.test=1").unwrap();
        assert_eq!(slots.get("cedar.test"), Some(&3));
        assert_eq!(slots.get("birch.test"), Some(&1));
        for bad in [
            "cedar.test",
            "cedar.test=0",
            "=2",
            "cedar.test=x",
            "a.test=1,a.test=2",
        ] {
            assert!(parse_source_slots(bad).is_err(), "{bad}");
        }
    }

    fn profile_env(key: &str) -> Option<String> {
        match key {
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
        assert_eq!(credentials["stellarlight"]["partner_key_present"], false);
        for source in credentials.as_object().unwrap().values() {
            assert!(source["present"]
                .as_object()
                .unwrap()
                .values()
                .all(serde_json::Value::is_boolean));
        }
    }
}
