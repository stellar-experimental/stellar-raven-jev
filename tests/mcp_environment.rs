use std::process::{Command, Stdio};

fn server(directory: &std::path::Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_stellar-raven-jev"));
    command
        .current_dir(directory)
        .args(["mcp", "--fixture", "--budget-usd", "0"])
        .arg("--output-dir")
        .arg(directory.join("runs"))
        .env_remove("JEV_ENV_FILE")
        .env_remove("JEV_BUDGET_USD")
        .env_remove("JEV_OUTPUT_DIR")
        .stdin(Stdio::null());
    command
}

#[test]
fn mcp_does_not_load_a_host_projects_environment_file() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join(".env"),
        "JEV_ENV_FILE=relative-untrusted-path\n",
    )
    .unwrap();
    let output = server(directory.path()).output().unwrap();
    assert!(output.status.success(), "{:?}", output.stderr);
    assert!(output.stdout.is_empty());
}

#[test]
fn explicit_mcp_environment_errors_do_not_echo_values() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("explicit.env");
    std::fs::write(&file, "BROKEN='private-test-marker\n").unwrap();
    let output = server(directory.path())
        .env("JEV_ENV_FILE", file)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("Cannot load the explicit JEV_ENV_FILE"));
    assert!(!error.contains("private-test-marker"));
}
