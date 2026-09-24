use serde_json::Value;
use std::process::Command;

fn cli() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_stellar-raven-jev"));
    // Strip host defaults so tests see the CLI defaults.
    command
        .env_remove("JEV_ENV_FILE")
        .env_remove("JEV_BUDGET_USD")
        .env_remove("JEV_OUTPUT_DIR")
        .env_remove("JEV_MAX_SEARCHES")
        .env_remove("JEV_PROVIDER_RPM");
    command
}

#[test]
fn a_busy_host_refuses_before_any_run_and_frees_the_slot_on_exit() {
    let temp = tempfile::tempdir().unwrap();
    let host = temp.path().join(".host");
    std::fs::create_dir_all(&host).unwrap();
    let slot = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(host.join("slot-0.lock"))
        .unwrap();
    slot.lock().unwrap();
    let run = || {
        cli()
            .args(["--fixture", "--output-dir"])
            .arg(temp.path())
            .args(["--max-searches", "1", "--admission-wait-secs", "0"])
            .args([
                "search",
                "How does the fictional Quillon ledger batch its receipts?",
                "--limit",
                "1",
            ])
            .output()
            .unwrap()
    };
    let refused = run();
    assert_eq!(refused.status.code(), Some(3));
    let busy: Value = serde_json::from_slice(&refused.stdout).unwrap();
    assert_eq!(busy["status"], "busy");
    assert!(busy["retry_after_ms"].as_u64().unwrap() > 0);
    // Nothing ran: the output directory holds only the host state folder.
    let entries: Vec<_> = std::fs::read_dir(temp.path()).unwrap().collect();
    assert_eq!(entries.len(), 1);
    drop(slot);
    let admitted = run();
    assert!(matches!(admitted.status.code(), Some(0 | 2)));
    let compact: Value = serde_json::from_slice(&admitted.stdout).unwrap();
    assert_eq!(compact["load"]["admission_wait_ms"], 0);
    assert_eq!(compact["load"]["degraded"], false);
}

#[test]
fn failed_initialization_still_returns_a_json_report_and_failure_exit() {
    let temp = tempfile::tempdir().unwrap();
    let output = cli()
        .current_dir(temp.path())
        // No Cloudflare settings: Jev cannot start, and the run fails at initialization.
        .env_remove("CLOUDFLARE_ACCOUNT_ID")
        .env_remove("CLOUDFLARE_API_TOKEN")
        .env_remove("JEV_CLOUDFLARE_AUTH_PROFILE")
        .args(["search", "Stellar RPC", "--resources", "agentic", "--json"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["status"], "failed");
    assert_eq!(report["counts"]["selected"], 0);
    assert_eq!(report["usage"]["requests"], 0);
    assert_eq!(report["reports"][0]["stage"], "initialization");
    assert_eq!(
        report["source_scope"]["excluded_source_ids"]
            .as_array()
            .unwrap()
            .len(),
        34
    );
}

#[test]
fn agentic_search_filters_before_routing_and_delivers_exact_text() {
    let temp = tempfile::tempdir().unwrap();
    let output = cli()
        .args(["--fixture", "--output-dir"])
        .arg(temp.path())
        .args([
            "search",
            "Find Stellar SDK documentation",
            "--resources",
            "agentic",
            "--json",
            "--full-text",
            "--limit",
            "1",
            "--full-record",
        ])
        .output()
        .unwrap();
    assert!(
        matches!(output.status.code(), Some(0 | 2)),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["mode"], "fixture");
    assert_eq!(report["usage"]["requests"], 0);
    let eligible = report["source_scope"]["eligible_source_ids"]
        .as_array()
        .unwrap();
    let excluded = report["source_scope"]["excluded_source_ids"]
        .as_array()
        .unwrap();
    assert_eq!(eligible.len(), 11);
    assert_eq!(excluded.len(), 34);
    assert!(eligible.contains(&serde_json::json!("stellarlight.skills")));
    assert!(excluded.contains(&serde_json::json!("lumenloop.articles")));
    let root = std::path::Path::new(report["directory"].as_str().unwrap());
    let routes: Value =
        serde_json::from_slice(&std::fs::read(root.join("routes.json")).unwrap()).unwrap();
    for pass in routes.as_array().unwrap() {
        assert_eq!(pass["scores"].as_array().unwrap().len(), eligible.len());
        for score in pass["scores"].as_array().unwrap() {
            assert!(eligible.contains(&score["source_id"]));
        }
    }
    let results = report["results"].as_array().unwrap();
    assert!(
        results.len() > 1,
        "The display limit must not cap JSON results"
    );
    // Each admitted document is stored once; text_path holds its exact text.
    let documents: Vec<Value> =
        serde_json::from_slice(&std::fs::read(root.join("documents.json")).unwrap()).unwrap();
    for row in results {
        assert!(eligible.contains(&row["source_id"]));
        let text = std::fs::read_to_string(row["text_path"].as_str().unwrap()).unwrap();
        let document = documents.iter().find(|d| d["id"] == row["id"]).unwrap();
        assert_eq!(row["text"], text);
        assert_eq!(document["text"], text);
        assert_eq!(row["text_bytes"], text.len());
        assert!(row.get("document_path").is_none());
    }
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(root.join("manifest.json")).unwrap()).unwrap();
    assert!(manifest["artifacts"]["files"]
        .as_array()
        .unwrap()
        .iter()
        .any(|a| a["path"] == "search.json"));
}

#[test]
fn default_search_prints_compact_json_and_keeps_a_light_record() {
    let temp = tempfile::tempdir().unwrap();
    let output = cli()
        .args(["--fixture", "--output-dir"])
        .arg(temp.path())
        .args(["search", "Stellar RPC events", "--limit", "1"])
        .output()
        .unwrap();
    assert!(matches!(output.status.code(), Some(0 | 2)));
    let compact: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(compact["compact"], true);
    assert_eq!(compact["results"].as_array().unwrap().len(), 1);
    let text_path = compact["results"][0]["text_path"].as_str().unwrap();
    assert!(std::path::Path::new(text_path).is_file());
    let report_path = std::path::Path::new(compact["full_report_path"].as_str().unwrap());
    let report: Value = serde_json::from_slice(&std::fs::read(report_path).unwrap()).unwrap();
    assert_eq!(
        report["source_scope"]["eligible_source_ids"]
            .as_array()
            .unwrap()
            .len(),
        45
    );
    assert!(report["results"].as_array().unwrap().len() > 1);
    assert!(report["results"][0].get("text").is_none());
    // The light record keeps only the report, its text files, and the manifest.
    let root = report_path.parent().unwrap();
    let mut names: Vec<String> = std::fs::read_dir(root)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    assert_eq!(names, ["manifest.json", "search-documents", "search.json"]);
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(root.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["record"], "light");
    assert_eq!(manifest["config"]["full_record"], false);
    // A light record cannot be replayed, and says why.
    let replay = cli().arg("report").arg(root).output().unwrap();
    assert_eq!(replay.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&replay.stderr).contains("--full-record"));
}

#[test]
fn configured_file_works_outside_project_and_missing_file_fails_without_run() {
    let temp = tempfile::tempdir().unwrap();
    let env = temp.path().join("explicit.env");
    std::fs::write(&env, "SEARCH_TEST_CONFIGURATION=fixture\n").unwrap();
    let output = cli()
        .current_dir(temp.path())
        .arg("--env-file")
        .arg(&env)
        .args([
            "--fixture",
            "search",
            "SDK docs",
            "--resources",
            "agentic",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(matches!(output.status.code(), Some(0 | 2)));
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(report["directory"]
        .as_str()
        .unwrap()
        .starts_with(temp.path().canonicalize().unwrap().to_str().unwrap()));
    let output = cli()
        .current_dir(temp.path())
        .arg("--env-file")
        .arg(temp.path().join("absent.env"))
        .args(["--fixture", "search", "SDK docs"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
}
