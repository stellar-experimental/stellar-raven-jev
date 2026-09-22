//! Black-box CLI tests for the service-v2 `operations` and `plan` commands.
//!
//! These tests encode docs/service-v2/CONTRACT.md. They run the compiled binary
//! in an empty working directory with provider credentials removed from the
//! environment. Fixture plans never access the network. No test in this file
//! makes a paid call; the live-mode tests assert rejection before spending.
//!
//! The commands exist only after the parent integrates src/operations.rs and
//! src/plan.rs. Until then every test here fails on the unknown subcommand,
//! which is the expected integration signal.

use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::{Command, Output},
};

const CREDENTIAL_VARS: &[&str] = &[
    "LUMENLOOP_API_KEY",
    "ALGOLIA_APPLICATION_ID_DOCS",
    "ALGOLIA_API_KEY_DOCS",
    "ALGOLIA_APPLICATION_ID_SITE",
    "ALGOLIA_API_KEY_SITE",
    "TYPESAFE_API_KEY",
    "CLOUDFLARE_API_TOKEN",
    "CLOUDFLARE_ACCOUNT_ID",
    "JEV_PROXY_URL",
    "JEV_ENV_FILE",
];

fn cli(directory: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_stellar-raven-jev"));
    command.current_dir(directory);
    for key in CREDENTIAL_VARS {
        command.env_remove(key);
    }
    command
}

fn run(directory: &Path, args: &[&str]) -> Output {
    cli(directory)
        .args(args)
        .output()
        .unwrap_or_else(|error| panic!("Cannot run the CLI with {args:?}: {error}"))
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn stdout_json(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "stdout must be JSON: {error}\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            stderr(output)
        )
    })
}

fn write_plan(directory: &Path, name: &str, plan: &Value) -> PathBuf {
    let path = directory.join(name);
    std::fs::write(&path, serde_json::to_vec_pretty(plan).unwrap()).unwrap();
    path
}

/// Immediate children of a directory, as name -> (is_dir, bytes for files).
fn listing(directory: &Path) -> BTreeMap<String, (bool, u64)> {
    if !directory.exists() {
        return BTreeMap::new();
    }
    std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            (
                entry.file_name().to_string_lossy().into_owned(),
                (metadata.is_dir(), metadata.len()),
            )
        })
        .collect()
}

/// Snapshot every file under a directory as relative path -> bytes.
fn snapshot(root: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut files = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(directory) = stack.pop() {
        for entry in std::fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else {
                files.insert(
                    path.strip_prefix(root)
                        .unwrap()
                        .to_string_lossy()
                        .into_owned(),
                    std::fs::read(&path).unwrap(),
                );
            }
        }
    }
    files
}

fn assert_no_new_run_directory(before: &BTreeMap<String, (bool, u64)>, output_dir: &Path) {
    let after = listing(output_dir);
    assert_eq!(
        &after, before,
        "A rejected or dry-run plan must not create run evidence"
    );
}

fn only_new_run_directory(before: &BTreeMap<String, (bool, u64)>, output_dir: &Path) -> PathBuf {
    let after = listing(output_dir);
    let new: Vec<_> = after
        .keys()
        .filter(|name| !before.contains_key(*name))
        .collect();
    assert_eq!(
        new.len(),
        1,
        "Exactly one new run directory must appear under the output directory: {new:?}"
    );
    let directory = output_dir.join(new[0]);
    assert!(
        directory.is_dir(),
        "The new output entry must be a directory"
    );
    directory
}

fn base_bounds() -> Value {
    json!({
        "max_calls": 4,
        "max_documents": 8,
        "max_http_requests": 32,
        "max_response_bytes": 1048576,
        "deadline_secs": 120,
        "max_spend_usd": 0.0
    })
}

fn connector_call(query: &str) -> Value {
    json!({
        "operation": "connector.search",
        "arguments": {"source_id": "stellarlight.projects", "query": query},
        "reason": "Baseline connector delegation keeps existing behavior.",
        "max_documents": 4,
        "max_pages": 1
    })
}

fn semantic_call(query: &str) -> Value {
    json!({
        "operation": "lumenloop.semantic",
        "arguments": {"query": query, "types": ["research"], "limit": 4},
        "reason": "Native semantic retrieval for editorial context.",
        "max_documents": 4,
        "max_pages": 1
    })
}

fn valid_plan() -> Value {
    json!({
        "schema_version": 1,
        "question": "Which Stellar projects provide smart contract tooling?",
        "requirements": ["Name projects", "Note evidence scope"],
        "bounds": base_bounds(),
        "calls": [
            connector_call("smart contract tooling"),
            semantic_call("smart contract tooling")
        ]
    })
}

fn plan_args(plan: &Path, extra: &[&str]) -> Vec<String> {
    let mut args: Vec<String> = vec!["plan".into(), plan.to_string_lossy().into_owned()];
    args.extend(extra.iter().map(|arg| (*arg).to_string()));
    args
}

fn run_fixture_plan(directory: &Path, plan: &Path, output_dir: &Path) -> Output {
    let args = plan_args(
        plan,
        &[
            "--fixture",
            "--budget-usd",
            "0",
            "--output-dir",
            &output_dir.to_string_lossy(),
        ],
    );
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    run(directory, &refs)
}

fn run_dry_plan(directory: &Path, plan: &Path, output_dir: &Path) -> Output {
    let args = plan_args(
        plan,
        &[
            "--dry-run",
            "--fixture",
            "--budget-usd",
            "0",
            "--output-dir",
            &output_dir.to_string_lossy(),
        ],
    );
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    run(directory, &refs)
}

/// Recursively collect sub-objects whose "operation" or "name" equals `name`.
fn find_operation<'a>(value: &'a Value, name: &str) -> Option<&'a Value> {
    if let Some(object) = value.as_object() {
        let labeled = object
            .get("operation")
            .or_else(|| object.get("name"))
            .and_then(Value::as_str)
            == Some(name);
        if labeled {
            return Some(value);
        }
        for child in object.values() {
            if let Some(found) = find_operation(child, name) {
                return Some(found);
            }
        }
    }
    if let Some(array) = value.as_array() {
        for child in array {
            if let Some(found) = find_operation(child, name) {
                return Some(found);
            }
        }
    }
    None
}

fn read_json(run_directory: &Path, name: &str) -> Value {
    let path = run_directory.join(name);
    serde_json::from_slice(
        &std::fs::read(&path).unwrap_or_else(|error| panic!("Cannot read {name}: {error}")),
    )
    .unwrap_or_else(|error| panic!("{name} must be valid JSON: {error}"))
}

fn assert_document_array(value: &Value, name: &str) {
    let documents = value
        .as_array()
        .unwrap_or_else(|| panic!("{name} must be a JSON array"));
    for document in documents {
        for field in [
            "id",
            "source_id",
            "title",
            "url",
            "text",
            "provenance",
            "raw_artifacts",
        ] {
            assert!(
                document.get(field).is_some(),
                "{name} documents must keep the Document field {field}: {document}"
            );
        }
    }
}

/// True when any saved artifact under the run directory contains the exact text.
fn artifacts_contain(run_directory: &Path, needle: &str) -> bool {
    snapshot(run_directory)
        .values()
        .any(|bytes| String::from_utf8_lossy(bytes).contains(needle))
}

#[test]
fn cli_exposes_operations_and_plan_commands() {
    let directory = tempfile::tempdir().unwrap();
    let output = run(directory.path(), &["--help"]);
    assert!(output.status.success(), "{}", stderr(&output));
    let help = String::from_utf8_lossy(&output.stdout);
    for command in ["operations", "plan"] {
        assert!(
            help.contains(command),
            "The CLI must expose a `{command}` command:\n{help}"
        );
    }
}

#[test]
fn operations_catalog_covers_all_operations_with_schemas_and_semantics() {
    let directory = tempfile::tempdir().unwrap();
    let output = run(directory.path(), &["operations", "--fixture"]);
    assert!(output.status.success(), "{}", stderr(&output));
    let catalog = stdout_json(&output);
    for (name, required_arguments) in [
        ("connector.search", vec!["source_id", "query"]),
        ("lumenloop.semantic", vec!["query", "types", "limit"]),
        ("scout.projects", vec!["limit"]),
    ] {
        let operation = find_operation(&catalog, name)
            .unwrap_or_else(|| panic!("The catalog must include {name}: {catalog}"));
        let schema = operation
            .get("arguments")
            .or_else(|| operation.get("input_schema"))
            .or_else(|| operation.get("schema"))
            .unwrap_or_else(|| panic!("{name} must publish an argument JSON schema"));
        let text = schema.to_string();
        for argument in required_arguments {
            assert!(
                text.contains(&format!("\"{argument}\"")),
                "{name} schema must name the argument {argument}: {text}"
            );
        }
        // Contract: catalog entries carry search semantics, provider limit,
        // text scope, and continuation support.
        let entry = operation.to_string();
        for (concept, needles) in [
            ("provider limit", &["limit"][..]),
            ("text scope", &["text_scope", "text scope", "text"][..]),
            (
                "continuation",
                &["continuation", "pagination", "offset", "pages"][..],
            ),
        ] {
            assert!(
                needles.iter().any(|needle| entry.contains(needle)),
                "{name} must describe its {concept}: {entry}"
            );
        }
    }
}

#[test]
fn plan_rejects_malformed_and_out_of_bounds_plans() {
    let mut plan = valid_plan();
    let cases: Vec<(&str, Value)> = vec![
        ("not-an-object", json!([1, 2, 3])),
        ("unknown-top-level-field", {
            let mut value = plan.clone();
            value["unexpected"] = json!(true);
            value
        }),
        ("unsupported-schema-version", {
            plan["schema_version"] = json!(2);
            plan.clone()
        }),
        ("empty-question", {
            plan["question"] = json!("   ");
            plan.clone()
        }),
        ("missing-bounds", {
            let mut value = valid_plan();
            value.as_object_mut().unwrap().remove("bounds");
            value
        }),
        ("zero-max-calls", {
            plan["bounds"]["max_calls"] = json!(0);
            plan.clone()
        }),
        ("calls-above-hard-ceiling", {
            plan["bounds"]["max_calls"] = json!(65);
            plan.clone()
        }),
        ("zero-max-documents", {
            plan["bounds"]["max_documents"] = json!(0);
            plan.clone()
        }),
        ("documents-above-score-ceiling", {
            plan["bounds"]["max_documents"] = json!(2001);
            plan.clone()
        }),
        ("http-requests-above-ceiling", {
            plan["bounds"]["max_http_requests"] = json!(4097);
            plan.clone()
        }),
        ("response-bytes-above-ceiling", {
            plan["bounds"]["max_response_bytes"] = json!(256 * 1024 * 1024 + 1);
            plan.clone()
        }),
        ("deadline-above-ceiling", {
            plan["bounds"]["deadline_secs"] = json!(3601);
            plan.clone()
        }),
        ("negative-spend", {
            plan["bounds"]["max_spend_usd"] = json!(-0.01);
            plan.clone()
        }),
        ("spend-above-hard-ceiling", {
            plan["bounds"]["max_spend_usd"] = json!(100.01);
            plan.clone()
        }),
        ("unknown-bounds-field", {
            plan["bounds"]["max_gpu_hours"] = json!(1);
            plan.clone()
        }),
        ("unknown-call-field", {
            plan["calls"][0]["retry"] = json!(true);
            plan.clone()
        }),
        ("unknown-operation", {
            plan["calls"][0]["operation"] = json!("shell.exec");
            plan.clone()
        }),
        ("unknown-operation-argument", {
            plan["calls"][0]["arguments"]["endpoint"] = json!("https://attacker.example/collect");
            plan.clone()
        }),
        ("call-allowance-above-global", {
            plan["bounds"]["max_documents"] = json!(4);
            plan["calls"][0]["max_documents"] = json!(5);
            plan.clone()
        }),
    ];
    for (name, value) in cases {
        let directory = tempfile::tempdir().unwrap();
        let output_dir = directory.path().join("runs");
        let plan_path = write_plan(directory.path(), "plan.json", &value);
        let before = listing(&output_dir);
        let output = run_fixture_plan(directory.path(), &plan_path, &output_dir);
        assert!(
            !output.status.success(),
            "{name}: the plan must be rejected\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            stderr(&output)
        );
        assert!(
            !output.stderr.is_empty(),
            "{name}: rejection must explain itself on stderr"
        );
        assert_no_new_run_directory(&before, &output_dir);
    }
    let directory = tempfile::tempdir().unwrap();
    let broken = directory.path().join("broken.json");
    std::fs::write(&broken, "{ not JSON").unwrap();
    let output_dir = directory.path().join("runs");
    let before = listing(&output_dir);
    let output = run_fixture_plan(directory.path(), &broken, &output_dir);
    assert!(!output.status.success());
    assert_no_new_run_directory(&before, &output_dir);
    let missing = run_fixture_plan(
        directory.path(),
        &directory.path().join("missing.json"),
        &output_dir,
    );
    assert!(!missing.status.success());
    assert_no_new_run_directory(&before, &output_dir);
}

#[test]
fn plan_rejects_live_spend_above_the_configured_allocation_before_spending() {
    let directory = tempfile::tempdir().unwrap();
    let mut plan = valid_plan();
    plan["bounds"]["max_spend_usd"] = json!(5.0);
    let plan_path = write_plan(directory.path(), "plan.json", &plan);
    let output_dir = directory.path().join("runs");
    let before = listing(&output_dir);
    // Live mode with a zero configured allocation must stop before any spend.
    let args = plan_args(&plan_path, &["--budget-usd", "0", "--output-dir"]);
    let mut args: Vec<String> = args;
    args.push(output_dir.to_string_lossy().into_owned());
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let output = run(directory.path(), &refs);
    assert!(
        !output.status.success(),
        "A live plan above the configured allocation must be rejected\nstdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let explanation = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        stderr(&output)
    )
    .to_lowercase();
    assert!(
        explanation.contains("spend") || explanation.contains("budget"),
        "The rejection must name the spend or budget constraint: {explanation}"
    );
    assert_no_new_run_directory(&before, &output_dir);
}

#[test]
fn plan_dry_run_validates_without_writing_evidence() {
    let directory = tempfile::tempdir().unwrap();
    let output_dir = directory.path().join("runs");
    let plan_path = write_plan(directory.path(), "plan.json", &valid_plan());
    let before = listing(&output_dir);
    let output = run_dry_plan(directory.path(), &plan_path, &output_dir);
    assert!(output.status.success(), "{}", stderr(&output));
    assert_no_new_run_directory(&before, &output_dir);
}

#[test]
fn plan_dry_run_still_rejects_a_malformed_plan() {
    let directory = tempfile::tempdir().unwrap();
    let output_dir = directory.path().join("runs");
    let mut plan = valid_plan();
    plan["calls"][0]["operation"] = json!("network.scan");
    let plan_path = write_plan(directory.path(), "plan.json", &plan);
    let before = listing(&output_dir);
    let output = run_dry_plan(directory.path(), &plan_path, &output_dir);
    assert!(!output.status.success());
    assert_no_new_run_directory(&before, &output_dir);
}

#[test]
fn plan_fixture_run_produces_the_publisher_evidence_shape() {
    let directory = tempfile::tempdir().unwrap();
    let output_dir = directory.path().join("runs");
    let plan_path = write_plan(directory.path(), "plan.json", &valid_plan());
    let before = listing(&output_dir);
    let output = run_fixture_plan(directory.path(), &plan_path, &output_dir);
    assert!(output.status.success(), "{}", stderr(&output));
    let run_directory = only_new_run_directory(&before, &output_dir);
    // Files the MCP publisher and the contract require.
    for name in [
        "documents.json",
        "scores.json",
        "selected.json",
        "uncertain.json",
        "rejected.json",
        "usage.json",
        "failures.json",
        "manifest.json",
    ] {
        assert!(
            run_directory.join(name).is_file(),
            "The run must write {name}"
        );
    }
    for name in [
        "documents.json",
        "selected.json",
        "uncertain.json",
        "rejected.json",
    ] {
        assert_document_array(&read_json(&run_directory, name), name);
    }
    let scores = read_json(&run_directory, "scores.json");
    for score in scores.as_array().expect("scores.json must be a JSON array") {
        assert!(score.get("document_id").is_some(), "{score}");
        assert!(score.get("probability").is_some(), "{score}");
        assert!(score.get("reason").is_some(), "{score}");
    }
    let usage = read_json(&run_directory, "usage.json");
    for field in ["requests", "input_tokens", "output_tokens", "cost_usd"] {
        assert!(usage.get(field).is_some(), "usage.json must keep {field}");
    }
    assert!(
        read_json(&run_directory, "failures.json").is_array(),
        "failures.json must be a JSON array"
    );
    let manifest = read_json(&run_directory, "manifest.json");
    assert!(manifest.get("schema_version").is_some(), "{manifest}");
    assert_eq!(
        manifest["answer_generated"],
        json!(false),
        "The service never generates answers: {manifest}"
    );
    assert!(
        manifest.to_string().contains("fixture"),
        "The manifest must record the offline fixture mode: {manifest}"
    );
    let status = manifest
        .get("outcome")
        .and_then(|outcome| outcome.get("status"))
        .or_else(|| manifest.get("status"))
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("The manifest must carry a run status: {manifest}"));
    assert!(
        ["complete", "partial", "failed"].contains(&status),
        "Unexpected run status {status}"
    );
    // Contract: preserve the original plan, call receipts, and HTTP metrics.
    assert!(
        artifacts_contain(&run_directory, "smart contract tooling"),
        "The run must preserve the original plan with its exact question"
    );
    assert!(
        artifacts_contain(&run_directory, "requests_started")
            || artifacts_contain(&run_directory, "retained_response_bytes")
            || artifacts_contain(&run_directory, "\"http\""),
        "The run must preserve HTTP metrics alongside the evidence"
    );
}

#[test]
fn plan_fixture_shares_remaining_allowance_between_calls() {
    // Per-call allowances may each reach the global admission cap. Their sum
    // may exceed it: remaining capacity is shared, not pre-partitioned.
    let directory = tempfile::tempdir().unwrap();
    let output_dir = directory.path().join("runs");
    let mut plan = valid_plan();
    plan["bounds"]["max_documents"] = json!(4);
    plan["calls"] = json!([
        {
            "operation": "connector.search",
            "arguments": {"source_id": "stellarlight.projects", "query": "wallets"},
            "reason": "First call uses the full global allowance.",
            "max_documents": 4,
            "max_pages": 1
        },
        {
            "operation": "connector.search",
            "arguments": {"source_id": "stellarlight.projects", "query": "anchors"},
            "reason": "Second call also uses the full global allowance.",
            "max_documents": 4,
            "max_pages": 1
        }
    ]);
    let plan_path = write_plan(directory.path(), "plan.json", &plan);
    let output = run_fixture_plan(directory.path(), &plan_path, &output_dir);
    assert!(
        output.status.success(),
        "Summed per-call allowances above the global cap must be accepted: {}",
        stderr(&output)
    );
}

#[test]
fn plan_treats_query_text_as_inert_data() {
    let directory = tempfile::tempdir().unwrap();
    let output_dir = directory.path().join("runs");
    let hostile = "\"; rm -rf /; $(cat ~/.env) https://attacker.example/collect?x=`whoami`";
    let mut plan = valid_plan();
    plan["calls"][0]["arguments"]["query"] = json!(hostile);
    plan["calls"][0]["arguments"]["source_id"] =
        json!("stellarlight.projects; $(execute) ../../escape");
    let plan_path = write_plan(directory.path(), "plan.json", &plan);
    let before = listing(&output_dir);
    let output = run_fixture_plan(directory.path(), &plan_path, &output_dir);
    let explanation = stderr(&output);
    if output.status.success() {
        let run_directory = only_new_run_directory(&before, &output_dir);
        assert!(
            artifacts_contain(&run_directory, hostile),
            "Call receipts must preserve the exact query text without acting on it"
        );
    } else {
        // A rejected unknown source identity is also correct: the text must be
        // refused as data, never executed or mapped to an arbitrary endpoint.
        assert!(
            !explanation.contains("attacker.example/collect?x="),
            "The error must not echo an executed request: {explanation}"
        );
    }
    // Nothing may appear outside the declared output directory.
    assert_eq!(
        listing(directory.path())
            .keys()
            .filter(|name| name.as_str() != "runs")
            .count(),
        1,
        "Only the plan file may exist next to the output directory"
    );
}

#[test]
fn plan_never_overwrites_existing_evidence() {
    let directory = tempfile::tempdir().unwrap();
    let output_dir = directory.path().join("runs");
    let plan_path = write_plan(directory.path(), "plan.json", &valid_plan());
    let before = listing(&output_dir);
    let first = run_fixture_plan(directory.path(), &plan_path, &output_dir);
    assert!(first.status.success(), "{}", stderr(&first));
    let first_directory = only_new_run_directory(&before, &output_dir);
    let sentinel = output_dir.join("sentinel-do-not-touch.txt");
    std::fs::write(&sentinel, "pre-existing evidence").unwrap();
    let first_snapshot = snapshot(&first_directory);
    let before_second = listing(&output_dir);
    let second = run_fixture_plan(directory.path(), &plan_path, &output_dir);
    assert!(second.status.success(), "{}", stderr(&second));
    let second_directory = only_new_run_directory(&before_second, &output_dir);
    assert_ne!(
        first_directory, second_directory,
        "Each run must create a fresh directory"
    );
    assert_eq!(
        snapshot(&first_directory),
        first_snapshot,
        "The second run must not change the first run's evidence"
    );
    assert_eq!(
        std::fs::read_to_string(&sentinel).unwrap(),
        "pre-existing evidence",
        "Existing files in the output directory must survive"
    );
}

#[test]
fn plan_fixture_mode_is_deterministic() {
    let directory = tempfile::tempdir().unwrap();
    let output_dir = directory.path().join("runs");
    let plan_path = write_plan(directory.path(), "plan.json", &valid_plan());
    let mut runs = Vec::new();
    for _ in 0..2 {
        let before = listing(&output_dir);
        let output = run_fixture_plan(directory.path(), &plan_path, &output_dir);
        assert!(output.status.success(), "{}", stderr(&output));
        runs.push(only_new_run_directory(&before, &output_dir));
    }
    for name in ["documents.json", "scores.json", "selected.json"] {
        assert_eq!(
            std::fs::read(runs[0].join(name)).unwrap(),
            std::fs::read(runs[1].join(name)).unwrap(),
            "Fixture {name} must be deterministic and offline"
        );
    }
}

#[test]
fn plan_preserves_distinct_records_and_a_duplicate_artifact() {
    // Two identical calls return identical fixture documents. The run must
    // keep the distinct records and merge duplicate provenance in a separate
    // artifact instead of silently dropping one copy.
    let directory = tempfile::tempdir().unwrap();
    let output_dir = directory.path().join("runs");
    let mut plan = valid_plan();
    plan["calls"] = json!([
        connector_call("repeat query"),
        connector_call("repeat query")
    ]);
    let plan_path = write_plan(directory.path(), "plan.json", &plan);
    let before = listing(&output_dir);
    let output = run_fixture_plan(directory.path(), &plan_path, &output_dir);
    assert!(output.status.success(), "{}", stderr(&output));
    let run_directory = only_new_run_directory(&before, &output_dir);
    let documents = read_json(&run_directory, "documents.json");
    let ids: Vec<_> = documents
        .as_array()
        .expect("documents.json must be a JSON array")
        .iter()
        .map(|document| document["id"].as_str().unwrap_or_default().to_string())
        .collect();
    let unique: std::collections::BTreeSet<_> = ids.iter().collect();
    assert_eq!(
        unique.len(),
        ids.len(),
        "Scoring IDs must be unique and stable: {ids:?}"
    );
    let duplicate_artifacts: Vec<_> = listing(&run_directory)
        .into_keys()
        .filter(|name| name.contains("duplicat"))
        .collect();
    assert!(
        !duplicate_artifacts.is_empty(),
        "The run must record merged duplicate provenance in a separate artifact"
    );
    for name in duplicate_artifacts {
        read_json(&run_directory, &name);
    }
}
