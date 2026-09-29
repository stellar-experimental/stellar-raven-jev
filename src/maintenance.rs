//! Run-folder upkeep: remove sessions nobody has touched for a while, and summarize use across the
//! sessions an output directory holds.

use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    path::Path,
    time::{Duration, SystemTime},
};

/// An automatic prune runs at most once in this interval per output directory.
const AUTO_PRUNE_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

/// A run folder is named `<unix seconds>-<uuid>`; nothing else in an output directory is pruned.
fn is_run_folder(name: &str) -> bool {
    let Some((stamp, id)) = name.split_once('-') else {
        return false;
    };
    !stamp.is_empty()
        && stamp.bytes().all(|b| b.is_ascii_digit())
        && uuid::Uuid::parse_str(id).is_ok()
}

/// The latest change to the folder or any entry directly inside it: a session continued by `more`
/// or `check` counts as active from its last call.
fn last_activity(dir: &Path) -> SystemTime {
    let mut latest = fs::metadata(dir)
        .and_then(|m| m.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH);
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Ok(modified) = entry.metadata().and_then(|m| m.modified()) {
                latest = latest.max(modified);
            }
        }
    }
    latest
}

fn folder_bytes(dir: &Path) -> u64 {
    let mut total = 0;
    let mut pending = vec![dir.to_path_buf()];
    while let Some(path) = pending.pop() {
        let Ok(entries) = fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            match entry.metadata() {
                Ok(meta) if meta.is_dir() => pending.push(entry.path()),
                Ok(meta) => total += meta.len(),
                Err(_) => {}
            }
        }
    }
    total
}

/// The entries of `output_dir`, or none when it does not exist because no search has run yet.
fn output_entries(output_dir: &Path) -> Result<Vec<fs::DirEntry>> {
    match fs::read_dir(output_dir) {
        Ok(entries) => Ok(entries.flatten().collect()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error)
            .with_context(|| format!("Cannot read the output directory {}", output_dir.display())),
    }
}

/// The session's lock when no call holds it; `None` while a call is running in the session.
fn idle_session(dir: &Path) -> Result<Option<Option<File>>> {
    let path = dir.join("session.lock");
    if !path.exists() {
        return Ok(Some(None));
    }
    let file = OpenOptions::new().write(true).open(&path)?;
    match file.try_lock() {
        Ok(()) => Ok(Some(Some(file))),
        Err(fs::TryLockError::WouldBlock) => Ok(None),
        Err(fs::TryLockError::Error(error)) => Err(error.into()),
    }
}

/// Remove every run folder in `output_dir` with no activity for `older_than`. A session that a call
/// holds is skipped. With `dry_run`, nothing is removed and the report says what would be.
pub fn prune(output_dir: &Path, older_than: Duration, dry_run: bool) -> Result<Value> {
    let now = SystemTime::now();
    let (mut removed, mut kept, mut busy, mut bytes) = (0u64, 0u64, 0u64, 0u64);
    for entry in output_entries(output_dir)? {
        let name = entry.file_name().to_string_lossy().into_owned();
        let path = entry.path();
        if !is_run_folder(&name) || !path.is_dir() {
            continue;
        }
        let age = now.duration_since(last_activity(&path)).unwrap_or_default();
        if age < older_than {
            kept += 1;
            continue;
        }
        let Some(_held) = idle_session(&path)? else {
            busy += 1;
            continue;
        };
        bytes += folder_bytes(&path);
        if !dry_run {
            fs::remove_dir_all(&path)
                .with_context(|| format!("Cannot remove {}", path.display()))?;
        }
        removed += 1;
    }
    Ok(json!({
        "output_dir": output_dir, "older_than_days": older_than.as_secs_f64() / 86_400.0,
        "dry_run": dry_run, "removed": removed, "kept": kept, "skipped_busy": busy,
        "bytes_freed": bytes,
    }))
}

/// Prune folders older than `retain_days` at most once a day per output directory, after a call has
/// printed its result. Zero turns it off. Only one process prunes at a time; the others skip.
pub fn auto_prune(output_dir: &Path, retain_days: u64) -> Result<Option<Value>> {
    if retain_days == 0 {
        return Ok(None);
    }
    let marker = output_dir.join(".last-prune");
    let recent = fs::metadata(&marker)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|at| SystemTime::now().duration_since(at).ok())
        .is_some_and(|since| since < AUTO_PRUNE_INTERVAL);
    if recent {
        return Ok(None);
    }
    let lock = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(output_dir.join(".prune.lock"))?;
    if lock.try_lock().is_err() {
        return Ok(None);
    }
    let report = prune(
        output_dir,
        Duration::from_secs(retain_days * 24 * 60 * 60),
        false,
    )?;
    fs::write(&marker, report.to_string())?;
    Ok(Some(report))
}

/// Totals over the run folders active in the last `days` days (0: all): sessions, calls, status,
/// Jev spend, source requests per host, capacity signals, and disk use.
pub fn usage(output_dir: &Path, days: u64) -> Result<Value> {
    let now = SystemTime::now();
    let window = Duration::from_secs(days * 24 * 60 * 60);
    let read = |path: &Path| -> Value {
        fs::read(path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or(Value::Null)
    };
    let mut sessions = 0u64;
    let mut calls = 0u64;
    let mut full_records = 0u64;
    let mut bytes = 0u64;
    let mut cost = 0.0f64;
    let mut jev_requests = 0u64;
    let mut degraded = 0u64;
    let mut cuts = 0u64;
    let mut fallbacks = 0u64;
    let mut statuses: BTreeMap<String, u64> = BTreeMap::new();
    let mut sources: BTreeMap<String, u64> = BTreeMap::new();
    for entry in output_entries(output_dir)? {
        let path = entry.path();
        if !is_run_folder(&entry.file_name().to_string_lossy()) || !path.is_dir() {
            continue;
        }
        if days > 0 && now.duration_since(last_activity(&path)).unwrap_or_default() > window {
            continue;
        }
        sessions += 1;
        bytes += folder_bytes(&path);
        full_records += u64::from(path.join("raw").is_dir());
        let session = read(&path.join("session.json"));
        calls += session["calls"].as_array().map_or(1, |c| c.len().max(1)) as u64;
        let status = read(&path.join("manifest.json"))["outcome"]["status"]
            .as_str()
            .unwrap_or("unfinished")
            .to_owned();
        *statuses.entry(status).or_default() += 1;
        let spent = read(&path.join("usage.json"));
        cost += spent["cost_usd"].as_f64().unwrap_or(0.0);
        jev_requests += spent["requests"].as_u64().unwrap_or(0);
        let load = &read(&path.join("search.json"))["load"];
        degraded += u64::from(load["degraded"] == true);
        cuts += load["sources_cut_at_deadline"].as_u64().unwrap_or(0);
        fallbacks += load["source_fallback_responses"].as_u64().unwrap_or(0);
        for (host, count) in load["source_requests"].as_object().into_iter().flatten() {
            *sources.entry(host.clone()).or_default() += count.as_u64().unwrap_or(0);
        }
    }
    Ok(json!({
        "output_dir": output_dir, "days": days, "sessions": sessions, "calls": calls,
        "statuses": statuses, "jev_cost_usd": (cost * 1e6).round() / 1e6,
        "jev_requests": jev_requests, "degraded_sessions": degraded,
        "sources_cut_at_deadline": cuts, "source_fallback_responses": fallbacks,
        "source_requests": sources, "full_records": full_records, "bytes": bytes,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_folder(root: &Path, stamp: u64) -> std::path::PathBuf {
        let dir = root.join(format!("{stamp}-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(dir.join("search-documents")).unwrap();
        fs::write(dir.join("search-documents/0000.txt"), "Fernlet text").unwrap();
        fs::write(
            dir.join("usage.json"),
            json!({"cost_usd":0.02,"requests":10}).to_string(),
        )
        .unwrap();
        fs::write(
            dir.join("manifest.json"),
            json!({"outcome":{"status":"partial"}}).to_string(),
        )
        .unwrap();
        fs::write(
            dir.join("search.json"),
            json!({"load":{"degraded":true,"sources_cut_at_deadline":2,
            "source_fallback_responses":1,"source_requests":{"cedar.test":7}}})
            .to_string(),
        )
        .unwrap();
        dir
    }

    fn age(dir: &Path, days: u64) {
        let past = SystemTime::now() - Duration::from_secs(days * 24 * 60 * 60);
        let mut paths: Vec<std::path::PathBuf> = fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        paths.push(dir.to_path_buf());
        for path in paths {
            File::options()
                .write(true)
                .open(&path)
                .or_else(|_| File::open(&path))
                .unwrap()
                .set_modified(past)
                .unwrap();
        }
    }

    #[test]
    fn prune_removes_only_idle_old_run_folders() {
        let root = tempfile::tempdir().unwrap();
        let old = run_folder(root.path(), 1_700_000_000);
        age(&old, 10);
        let fresh = run_folder(root.path(), 1_790_000_000);
        let held = run_folder(root.path(), 1_700_000_001);
        let lock = File::create(held.join("session.lock")).unwrap();
        age(&held, 10);
        lock.try_lock().unwrap();
        // Host state and other names are never touched, however old.
        fs::create_dir_all(root.path().join(".host")).unwrap();
        fs::create_dir_all(root.path().join("notes")).unwrap();
        let week = Duration::from_secs(7 * 24 * 60 * 60);
        let dry = prune(root.path(), week, true).unwrap();
        assert_eq!(
            (dry["removed"].as_u64(), dry["kept"].as_u64()),
            (Some(1), Some(1))
        );
        assert_eq!(dry["skipped_busy"], 1);
        assert!(old.is_dir(), "a dry run removes nothing");
        assert!(dry["bytes_freed"].as_u64().unwrap() > 0);
        let done = prune(root.path(), week, false).unwrap();
        assert_eq!(done["removed"], 1);
        assert!(!old.exists() && fresh.is_dir() && held.is_dir());
        assert!(root.path().join(".host").is_dir() && root.path().join("notes").is_dir());
        drop(lock);
        assert_eq!(prune(root.path(), week, false).unwrap()["removed"], 1);
    }

    #[test]
    fn auto_prune_runs_at_most_once_a_day_and_zero_turns_it_off() {
        let root = tempfile::tempdir().unwrap();
        let old = run_folder(root.path(), 1_700_000_000);
        age(&old, 10);
        assert!(auto_prune(root.path(), 0).unwrap().is_none());
        assert!(old.is_dir());
        let first = auto_prune(root.path(), 7)
            .unwrap()
            .expect("first prune runs");
        assert_eq!(first["removed"], 1);
        let again = run_folder(root.path(), 1_700_000_002);
        age(&again, 10);
        assert!(auto_prune(root.path(), 7).unwrap().is_none(), "once a day");
        assert!(again.is_dir());
    }

    #[test]
    fn usage_sums_sessions_in_the_window() {
        let root = tempfile::tempdir().unwrap();
        let old = run_folder(root.path(), 1_700_000_000);
        age(&old, 10);
        run_folder(root.path(), 1_790_000_000);
        let recent = usage(root.path(), 7).unwrap();
        assert_eq!(recent["sessions"], 1);
        assert_eq!(recent["statuses"]["partial"], 1);
        assert_eq!(recent["source_requests"]["cedar.test"], 7);
        assert_eq!(recent["degraded_sessions"], 1);
        let all = usage(root.path(), 0).unwrap();
        assert_eq!(all["sessions"], 2);
        assert_eq!(all["jev_cost_usd"], 0.04);
    }

    #[test]
    fn a_missing_output_directory_counts_as_no_sessions() {
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("never-created");
        assert_eq!(usage(&missing, 0).unwrap()["sessions"], 0);
        let pruned = prune(&missing, Duration::from_secs(1), true).unwrap();
        assert_eq!(pruned["removed"], 0);
        assert!(!missing.exists());
    }
}
