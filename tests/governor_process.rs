use std::{
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use stellar_raven_jev::governor::{Governor, ProviderBudget};

struct Workers(Vec<Child>);

impl Drop for Workers {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn wait_until(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !ready() {
        assert!(Instant::now() < deadline, "worker deadline exceeded");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn spend_in_worker(root: &Path, worker: &str) {
    let governor = Governor::at(&root.join("host")).unwrap();
    let provider = ProviderBudget {
        key: "fernlet".into(),
        // One initial send, with one minute before another send becomes available.
        per_minute: 1.0,
    };
    std::fs::write(root.join(format!("ready-{worker}")), b"").unwrap();
    wait_until(|| root.join("start").exists());
    let spent = (0..50)
        .filter(|_| governor.consume(&provider).unwrap())
        .count();
    std::fs::write(root.join(format!("spent-{worker}")), spent.to_string()).unwrap();
}

#[test]
fn provider_budget_is_shared_across_processes() {
    if let Some(root) = std::env::var_os("JEV_TEST_GOVERNOR_DIRECTORY") {
        let worker = std::env::var("JEV_TEST_GOVERNOR_WORKER").unwrap();
        spend_in_worker(Path::new(&root), &worker);
        return;
    }

    let root = tempfile::tempdir().unwrap();
    let executable = std::env::current_exe().unwrap();
    let mut workers = Workers(Vec::new());
    for worker in 0..4 {
        workers.0.push(
            Command::new(&executable)
                .args(["--exact", "provider_budget_is_shared_across_processes"])
                .env_clear()
                .env("JEV_TEST_GOVERNOR_DIRECTORY", root.path())
                .env("JEV_TEST_GOVERNOR_WORKER", worker.to_string())
                .stdout(Stdio::null())
                .spawn()
                .unwrap(),
        );
    }
    wait_until(|| (0..4).all(|worker| root.path().join(format!("ready-{worker}")).exists()));
    std::fs::write(root.path().join("start"), b"").unwrap();
    wait_until(|| {
        workers.0.iter_mut().all(|child| {
            child.try_wait().unwrap().is_some_and(|status| {
                assert!(status.success(), "worker failed: {status}");
                true
            })
        })
    });
    let spent: usize = (0..4)
        .map(|worker| {
            std::fs::read_to_string(root.path().join(format!("spent-{worker}")))
                .unwrap()
                .parse::<usize>()
                .unwrap()
        })
        .sum();
    assert_eq!(spent, 1, "all four processes share one provider allowance");
}
