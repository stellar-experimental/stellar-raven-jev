//! Host-wide coordination between concurrent searches.
//!
//! Separate CLI processes on one host share three things through a lock-protected state file in
//! one directory: admission slots (how many searches run at once), Jev provider send budgets and
//! cooldowns, and source rate limits (refusal cooldowns and advertised request windows). Without a
//! directory, the same state is process-local. The lock is held only while the state is read and
//! replaced, never during a network call, and the state file is replaced atomically.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// A provider's send budget refills continuously; it can hold this many seconds of its rate.
const BURST_SECONDS: f64 = 10.0;
/// Longest single wait a caller is told to take before it asks again.
const MAX_WAIT: Duration = Duration::from_secs(5);
/// The assumed window length until replies show a longer one.
const DEFAULT_WINDOW_MS: u64 = 60_000;
/// A reply whose reset is this much earlier than the current window's belongs to an older window.
const WINDOW_RESET_TOLERANCE_MS: u64 = 2_000;

#[derive(Default, Serialize, Deserialize)]
struct State {
    providers: BTreeMap<String, Bucket>,
    /// Source scope (host and path) to the Unix time in milliseconds when it opens again.
    gates: BTreeMap<String, u64>,
    /// Source scopes that advertise a request window. The learned limit is kept across windows.
    windows: BTreeMap<String, Window>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Bucket {
    tokens: f64,
    at_ms: u64,
    cooling_until_ms: u64,
}

/// A fixed request window that a source advertises (`x-ratelimit-limit` and
/// `x-ratelimit-reset`), counted across every process on the host. When a window ends, the next
/// one is predicted from the learned length (`period_ms`), so counting never stops, and bookings
/// that were never sent expire with their window. Replies correct the prediction.
#[derive(Clone, Serialize, Deserialize)]
struct Window {
    limit: u64,
    reset_ms: u64,
    used: u64,
    period_ms: u64,
    /// Counts windows. It changes only when a window ends, so a booking names its window even
    /// when replies move the reset.
    epoch: u64,
    /// Bookings are spread evenly: the next one may start at this time.
    paced_until_ms: u64,
}

impl Window {
    fn roll(&mut self, now: u64) {
        if now >= self.reset_ms {
            let periods = (now - self.reset_ms) / self.period_ms + 1;
            self.reset_ms += periods * self.period_ms;
            self.epoch += periods;
            self.used = 0;
        }
    }

    fn wait(&self, now: u64) -> Duration {
        Duration::from_millis(self.reset_ms.saturating_sub(now))
    }
}

/// What a source response said about its rate limits.
#[derive(Clone, Debug, Default)]
pub struct SourceSignals {
    pub status: u16,
    pub retry_after: Option<Duration>,
    pub limit: Option<u64>,
    pub remaining: Option<u64>,
    /// When the current window ends, as Unix milliseconds.
    pub reset_ms: Option<u64>,
}

/// One Jev provider as the governor sees it: a stable identity and a send rate.
#[derive(Clone, Debug)]
pub struct ProviderBudget {
    /// Provider name plus a digest of the account or key, so separate credentials keep separate
    /// budgets. Never a credential.
    pub key: String,
    pub per_minute: f64,
}

/// Which provider a call should use.
#[derive(Debug, PartialEq)]
pub enum Send {
    /// The provider at this chain index is outside its cooldown and has budget now.
    Go(usize),
    /// No usable provider has budget now; ask again after this long.
    Wait(Duration),
    /// No provider is usable at all.
    None,
}

/// Requests booked in one scope's window: how many, and which window.
#[derive(Clone, Debug, PartialEq)]
pub struct Booked {
    pub scope: String,
    pub count: u64,
    pub window_epoch: u64,
}

/// The result of booking a question's requests across source scopes.
#[derive(Debug, PartialEq)]
pub enum Booking {
    /// Every scope had room; one entry for each scope that advertises a window.
    Booked(Vec<Booked>),
    /// Some scope has no room; nothing was booked. Ask again after this long.
    Wait(Duration),
}

pub struct Governor {
    dir: Option<PathBuf>,
    memory: Mutex<State>,
}

/// A held admission slot. Dropping it, or the process exiting, frees the slot.
pub struct Admission {
    _slot: Option<File>,
    pub waited_ms: u64,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl Governor {
    /// Process-local state only.
    pub fn local() -> Self {
        Self {
            dir: None,
            memory: Mutex::default(),
        }
    }

    /// State shared by every process that uses `dir`.
    pub fn at(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("Cannot create the host state folder {}", dir.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
        }
        // Absolute, so every working directory names the same host.
        let dir = dir
            .canonicalize()
            .with_context(|| format!("Cannot resolve the host state folder {}", dir.display()))?;
        Ok(Self {
            dir: Some(dir),
            memory: Mutex::default(),
        })
    }

    /// Read, change, and replace the state under an exclusive lock. The state is replaced by a
    /// rename, so a process that dies leaves the old or the new state, never a partial one.
    fn update<T>(&self, change: impl FnOnce(&mut State, u64) -> T) -> Result<T> {
        let Some(dir) = &self.dir else {
            let mut state = self.memory.lock().unwrap_or_else(|e| e.into_inner());
            return Ok(change(&mut state, now_ms()));
        };
        let lock = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join("state.lock"))
            .context("Cannot open the host state lock")?;
        lock.lock().context("Cannot lock the host state")?;
        let path = dir.join("state.json");
        let text = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error).context("Cannot read the host state"),
        };
        let mut state: State = if text.is_empty() {
            State::default()
        } else {
            match serde_json::from_slice(&text) {
                Ok(state) => state,
                Err(error) => {
                    // Damaged outside this program (writes are atomic). Keep it for inspection,
                    // fail this call, and let the next call start from an empty state.
                    let aside = dir.join(format!("state.json.damaged-{}", now_ms()));
                    let _ = std::fs::rename(&path, &aside);
                    anyhow::bail!(
                        "The host state was damaged and moved to {}: {error}",
                        aside.display()
                    );
                }
            }
        };
        let result = change(&mut state, now_ms());
        let bytes = serde_json::to_vec(&state)?;
        if bytes != text {
            let temporary = dir.join(format!("state.json.{}", std::process::id()));
            let mut file = File::create(&temporary).context("Cannot write the host state")?;
            file.write_all(&bytes)
                .context("Cannot write the host state")?;
            std::fs::rename(&temporary, &path).context("Cannot replace the host state")?;
        }
        Ok(result)
    }

    /// The first provider in chain order that is usable, is not cooling, and has budget now,
    /// preferring one other than `avoid`. Nothing is spent: `consume` spends at the send.
    pub fn choose(
        &self,
        providers: &[ProviderBudget],
        usable: &[bool],
        avoid: Option<usize>,
    ) -> Result<Send> {
        self.update(|state, now| {
            let mut ready = Vec::new();
            let mut soonest: Option<u64> = None;
            for (i, provider) in providers.iter().enumerate() {
                if !usable.get(i).copied().unwrap_or(false) {
                    continue;
                }
                let bucket = refill(state, provider, now);
                let wait = if bucket.cooling_until_ms > now {
                    bucket.cooling_until_ms - now
                } else if bucket.tokens >= 1.0 {
                    ready.push(i);
                    continue;
                } else {
                    ((1.0 - bucket.tokens) * 60_000.0 / provider.per_minute).ceil() as u64
                };
                soonest = Some(soonest.map_or(wait, |s| s.min(wait)));
            }
            let chosen = ready
                .iter()
                .copied()
                .find(|&i| Some(i) != avoid)
                .or_else(|| ready.first().copied());
            match (chosen, soonest) {
                (Some(i), _) => Send::Go(i),
                (None, Some(wait)) => Send::Wait(Duration::from_millis(wait.max(1)).min(MAX_WAIT)),
                (None, None) => Send::None,
            }
        })
    }

    /// Spend one send from a provider's budget, at the moment of sending. False when the provider
    /// is cooling or its budget is empty.
    pub fn consume(&self, provider: &ProviderBudget) -> Result<bool> {
        self.update(|state, now| {
            let bucket = refill(state, provider, now);
            if bucket.cooling_until_ms > now || bucket.tokens < 1.0 {
                return false;
            }
            bucket.tokens -= 1.0;
            true
        })
    }

    /// Cool a provider for every process. A shorter wait never cuts an existing cooldown short.
    pub fn cool(&self, provider: &ProviderBudget, wait: Duration) -> Result<()> {
        self.update(|state, now| {
            let bucket = refill(state, provider, now);
            let until = now.saturating_add(wait.as_millis() as u64);
            bucket.cooling_until_ms = bucket.cooling_until_ms.max(until);
        })
    }

    /// Ask to send one request to a source scope, at the moment of sending. `None` means go. A
    /// request booked in the scope's current window (`booked_epoch`) is not
    /// counted again; any other request counts against the window. `Some(wait)` means the scope is
    /// closed for that long, by a refusal or because this host used the whole window.
    pub fn source_ticket(
        &self,
        scope: &str,
        booked_epoch: Option<u64>,
    ) -> Result<Option<Duration>> {
        self.update(|state, now| {
            state.gates.retain(|_, until| *until > now);
            if let Some(until) = state.gates.get(scope) {
                return Some(Duration::from_millis(until - now));
            }
            let window = state.windows.get_mut(scope)?;
            window.roll(now);
            if booked_epoch == Some(window.epoch) {
                return None;
            }
            if window.used >= window.limit {
                return Some(window.wait(now));
            }
            window.used += 1;
            None
        })
    }

    /// Book a question's requests in every scope's current window at once, or none of them. A
    /// scope that advertises no window needs no booking. A question never books more than one
    /// whole window of a scope. Bookings are also spread across the window in proportion to their
    /// size, so the source receives questions one after another rather than in a burst when a
    /// window opens. A scope that a refusal closed has no room until it opens again.
    pub fn book_windows(&self, demands: &[(String, u64)]) -> Result<Booking> {
        self.update(|state, now| {
            let mut wait = Duration::ZERO;
            state.gates.retain(|_, until| *until > now);
            for (scope, count) in demands {
                // A scope closed by a refusal has no room, windowed or not.
                if let Some(until) = state.gates.get(scope) {
                    wait = wait.max(Duration::from_millis(until - now));
                }
                if let Some(window) = state.windows.get_mut(scope) {
                    window.roll(now);
                    if window.used + (*count).min(window.limit) > window.limit {
                        wait = wait.max(window.wait(now));
                    }
                    if window.paced_until_ms > now {
                        wait = wait.max(Duration::from_millis(window.paced_until_ms - now));
                    }
                }
            }
            if !wait.is_zero() {
                return Booking::Wait(wait);
            }
            let mut booked = Vec::new();
            for (scope, count) in demands {
                if let Some(window) = state.windows.get_mut(scope) {
                    let count = (*count).min(window.limit);
                    window.used += count;
                    window.paced_until_ms =
                        window.paced_until_ms.max(now) + count * window.period_ms / window.limit;
                    booked.push(Booked {
                        scope: scope.clone(),
                        count,
                        window_epoch: window.epoch,
                    });
                }
            }
            Booking::Booked(booked)
        })
    }

    /// Record what a source response said. A refusal closes the scope for its Retry-After. An
    /// advertised window is learned and kept: within a window the larger of this host's count and
    /// the source's count holds, a later reset extends the window, a reply that names an earlier
    /// reset belongs to an older window and changes nothing, and the longest remaining time seen
    /// becomes the window length used to predict the next window.
    pub fn observe_source(
        &self,
        scope: &str,
        signals: &SourceSignals,
        default_close: Duration,
    ) -> Result<()> {
        self.update(|state, now| {
            if signals.status == 429 {
                let wait = signals.retry_after.unwrap_or(default_close);
                let until = now.saturating_add(wait.as_millis() as u64);
                let entry = state.gates.entry(scope.to_owned()).or_insert(0);
                *entry = (*entry).max(until);
            }
            let (Some(limit), Some(reset_ms)) = (signals.limit, signals.reset_ms) else {
                return;
            };
            if reset_ms <= now || limit == 0 {
                return;
            }
            let reported = limit.saturating_sub(signals.remaining.unwrap_or(limit));
            let window = state.windows.entry(scope.to_owned()).or_insert(Window {
                limit,
                reset_ms,
                used: 0,
                period_ms: DEFAULT_WINDOW_MS,
                epoch: 0,
                paced_until_ms: 0,
            });
            window.roll(now);
            if reset_ms + WINDOW_RESET_TOLERANCE_MS < window.reset_ms {
                return;
            }
            window.reset_ms = window.reset_ms.max(reset_ms);
            window.period_ms = window.period_ms.max(reset_ms - now);
            window.limit = limit;
            window.used = window.used.max(reported);
        })
    }

    /// Wait up to `wait` for one of `slots` admission slots. Without a state folder every search
    /// is admitted.
    pub async fn admit(&self, slots: usize, wait: Duration) -> Result<Option<Admission>> {
        let Some(dir) = &self.dir else {
            return Ok(Some(Admission {
                _slot: None,
                waited_ms: 0,
            }));
        };
        let started = std::time::Instant::now();
        loop {
            for i in 0..slots.max(1) {
                let file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(dir.join(format!("slot-{i}.lock")))
                    .context("Cannot open an admission slot")?;
                match file.try_lock() {
                    Ok(()) => {
                        return Ok(Some(Admission {
                            _slot: Some(file),
                            waited_ms: started.elapsed().as_millis() as u64,
                        }))
                    }
                    Err(std::fs::TryLockError::WouldBlock) => {}
                    Err(std::fs::TryLockError::Error(error)) => {
                        return Err(error).context("Cannot lock an admission slot")
                    }
                }
            }
            if started.elapsed() >= wait {
                return Ok(None);
            }
            // Spread the polls so waiting searches do not wake together.
            let spread = uuid::Uuid::new_v4().as_u128() % 200;
            tokio::time::sleep(Duration::from_millis(150 + spread as u64)).await;
        }
    }
}

/// The provider's bucket, refilled to `now`. A new bucket starts full.
fn refill<'a>(state: &'a mut State, provider: &ProviderBudget, now: u64) -> &'a mut Bucket {
    let capacity = (provider.per_minute / 60.0 * BURST_SECONDS).max(1.0);
    let bucket = state
        .providers
        .entry(provider.key.clone())
        .or_insert(Bucket {
            tokens: capacity,
            at_ms: now,
            cooling_until_ms: 0,
        });
    let elapsed = now.saturating_sub(bucket.at_ms) as f64;
    bucket.tokens = (bucket.tokens + elapsed * provider.per_minute / 60_000.0).min(capacity);
    bucket.at_ms = bucket.at_ms.max(now);
    bucket
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(key: &str, per_minute: f64) -> ProviderBudget {
        ProviderBudget {
            key: key.into(),
            per_minute,
        }
    }

    fn window(limit: u64, remaining: u64, reset_ms: u64) -> SourceSignals {
        SourceSignals {
            status: 200,
            limit: Some(limit),
            remaining: Some(remaining),
            reset_ms: Some(reset_ms),
            ..Default::default()
        }
    }

    #[test]
    fn sends_spill_to_the_next_provider_when_the_first_budget_is_spent() {
        let governor = Governor::local();
        // 60 per minute holds a 10-second burst of 10 sends.
        let providers = [budget("a", 60.0), budget("b", 600.0)];
        let usable = [true, true];
        for _ in 0..10 {
            assert_eq!(
                governor.choose(&providers, &usable, None).unwrap(),
                Send::Go(0)
            );
            assert!(governor.consume(&providers[0]).unwrap());
        }
        assert!(!governor.consume(&providers[0]).unwrap());
        assert_eq!(
            governor.choose(&providers, &usable, None).unwrap(),
            Send::Go(1)
        );
    }

    #[test]
    fn choosing_spends_nothing_and_a_spent_budget_waits_for_the_next_token() {
        let governor = Governor::local();
        let providers = [budget("a", 6.0)];
        for _ in 0..3 {
            assert_eq!(
                governor.choose(&providers, &[true], None).unwrap(),
                Send::Go(0)
            );
        }
        assert!(governor.consume(&providers[0]).unwrap());
        match governor.choose(&providers, &[true], None).unwrap() {
            Send::Wait(wait) => assert!(wait > Duration::ZERO && wait <= MAX_WAIT),
            other => panic!("expected a wait, got {other:?}"),
        }
    }

    #[test]
    fn a_cooling_or_unusable_provider_is_skipped_and_avoid_is_a_preference() {
        let governor = Governor::local();
        let providers = [budget("a", 600.0), budget("b", 600.0)];
        let choose = |usable: &[bool], avoid| governor.choose(&providers, usable, avoid).unwrap();
        assert_eq!(choose(&[true, true], Some(0)), Send::Go(1));
        assert_eq!(choose(&[true, false], Some(0)), Send::Go(0));
        governor
            .cool(&providers[0], Duration::from_secs(30))
            .unwrap();
        assert!(!governor.consume(&providers[0]).unwrap());
        assert_eq!(choose(&[true, true], None), Send::Go(1));
        assert!(matches!(choose(&[true, false], None), Send::Wait(_)));
        assert_eq!(choose(&[false, false], None), Send::None);
    }

    #[test]
    fn concurrent_processes_never_spend_more_than_the_budget() {
        let dir = tempfile::tempdir().unwrap();
        let provider = budget("a", 60.0);
        let spent: usize = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..4)
                .map(|_| {
                    let governor = Governor::at(dir.path()).unwrap();
                    let provider = provider.clone();
                    scope.spawn(move || {
                        (0..50)
                            .filter(|_| governor.consume(&provider).unwrap())
                            .count()
                    })
                })
                .collect();
            workers.into_iter().map(|w| w.join().unwrap()).sum()
        });
        // A 10-second burst of a 60-per-minute budget, plus at most one refill during the test.
        assert!((10..=11).contains(&spent), "spent {spent}");
    }

    #[test]
    fn processes_sharing_a_folder_share_cooldowns_gates_and_slots() {
        let dir = tempfile::tempdir().unwrap();
        let first = Governor::at(dir.path()).unwrap();
        let second = Governor::at(dir.path()).unwrap();
        let provider = budget("a", 600.0);
        first.cool(&provider, Duration::from_secs(30)).unwrap();
        assert!(!second.consume(&provider).unwrap());
        let refused = SourceSignals {
            status: 429,
            retry_after: Some(Duration::from_secs(3_600)),
            ..Default::default()
        };
        first
            .observe_source("example.org/api", &refused, Duration::from_secs(10))
            .unwrap();
        let wait = second
            .source_ticket("example.org/api", None)
            .unwrap()
            .unwrap();
        assert!(
            wait > Duration::from_secs(3_500),
            "a long Retry-After is kept"
        );
        assert!(second
            .source_ticket("example.org/other", None)
            .unwrap()
            .is_none());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let held = first
                .admit(1, Duration::ZERO)
                .await
                .unwrap()
                .expect("a free slot");
            assert!(second
                .admit(1, Duration::from_millis(300))
                .await
                .unwrap()
                .is_none());
            drop(held);
            assert!(second.admit(1, Duration::ZERO).await.unwrap().is_some());
        });
    }

    #[test]
    fn a_learned_window_closes_when_used_and_opens_only_in_a_new_window() {
        let governor = Governor::local();
        let scope = "example.org/search";
        let now = now_ms();
        let reset = now + 60_000;
        let ticket = |booked| governor.source_ticket(scope, booked).unwrap();
        // Before any response, the scope is unknown and open.
        assert!(ticket(None).is_none());
        governor
            .observe_source(scope, &window(3, 2, reset), Duration::from_secs(10))
            .unwrap();
        assert!(ticket(None).is_none());
        assert!(ticket(None).is_none());
        assert!(ticket(None).unwrap() > Duration::from_secs(50));
        // A relative reset that moves a little later, and a late reply from an older window, do
        // not reopen the window.
        governor
            .observe_source(scope, &window(3, 3, reset + 3_000), Duration::from_secs(10))
            .unwrap();
        governor
            .observe_source(scope, &window(3, 3, now + 1_000), Duration::from_secs(10))
            .unwrap();
        assert!(ticket(None).is_some());
    }

    #[test]
    fn a_window_that_ends_rolls_to_a_predicted_one_and_keeps_counting() {
        let governor = Governor::local();
        let scope = "example.org/search";
        let now = now_ms();
        governor
            .observe_source(scope, &window(2, 2, now + 20), Duration::from_secs(10))
            .unwrap();
        std::thread::sleep(Duration::from_millis(40));
        // The window ended with no reply about the next one: the learned limit still counts,
        // and the wait runs to the predicted next reset, not forever.
        let ticket = |booked| governor.source_ticket(scope, booked).unwrap();
        assert!(ticket(None).is_none());
        assert!(ticket(None).is_none());
        let wait = ticket(None).expect("the predicted window is used up");
        assert!(wait <= Duration::from_millis(DEFAULT_WINDOW_MS));
    }

    #[test]
    fn a_question_books_every_scope_or_none_and_bookings_expire_with_their_window() {
        let governor = Governor::local();
        let (a, b) = ("example.org/a", "example.org/b");
        let demands = [(a.to_owned(), 4), (b.to_owned(), 4)];
        // Scopes without a window need no booking.
        assert_eq!(
            governor.book_windows(&demands).unwrap(),
            Booking::Booked(vec![])
        );
        let now = now_ms();
        governor
            .observe_source(a, &window(10, 10, now + 60_000), Duration::from_secs(10))
            .unwrap();
        governor
            .observe_source(b, &window(5, 5, now + 60_000), Duration::from_secs(10))
            .unwrap();
        let Booking::Booked(first) = governor.book_windows(&demands).unwrap() else {
            panic!("room for the first question");
        };
        assert_eq!(first.len(), 2);
        // Scope b has one slot left, so the second question books nothing anywhere.
        assert!(matches!(
            governor.book_windows(&demands).unwrap(),
            Booking::Wait(_)
        ));
        assert_eq!(governor.source_ticket(a, None).unwrap(), None);
        // A booked request in its window goes without counting; a stale booking counts.
        let epoch = first[1].window_epoch;
        // A reply that moves the reset keeps the window, so the booking still holds.
        governor
            .observe_source(b, &window(5, 1, now + 61_000), Duration::from_secs(10))
            .unwrap();
        assert_eq!(governor.source_ticket(b, Some(epoch)).unwrap(), None);
        assert_eq!(governor.source_ticket(b, Some(epoch + 1)).unwrap(), None);
        assert!(governor
            .source_ticket(b, Some(epoch + 1))
            .unwrap()
            .is_some());
    }

    #[test]
    fn a_refused_scope_has_no_room_for_a_booking_with_or_without_a_window() {
        let governor = Governor::local();
        let refused = |limit: Option<u64>| SourceSignals {
            status: 429,
            retry_after: Some(Duration::from_secs(30)),
            limit,
            remaining: limit.map(|l| l - 1),
            reset_ms: limit.map(|_| now_ms() + 60_000),
        };
        governor
            .observe_source("example.org/a", &refused(Some(60)), Duration::from_secs(10))
            .unwrap();
        governor
            .observe_source("example.org/b", &refused(None), Duration::from_secs(10))
            .unwrap();
        for scope in ["example.org/a", "example.org/b"] {
            match governor.book_windows(&[(scope.to_owned(), 14)]).unwrap() {
                Booking::Wait(wait) => assert!(wait > Duration::from_secs(25), "{scope}"),
                other => panic!("{scope}: expected a wait, got {other:?}"),
            }
        }
        // Nothing was booked while the scopes were closed.
        assert!(matches!(
            governor
                .book_windows(&[("example.org/c".to_owned(), 1)])
                .unwrap(),
            Booking::Booked(_)
        ));
    }

    #[test]
    fn bookings_are_spread_across_the_window() {
        let governor = Governor::local();
        let scope = "example.org/search";
        governor
            .observe_source(
                scope,
                &window(60, 60, now_ms() + 60_000),
                Duration::from_secs(10),
            )
            .unwrap();
        let demand = [(scope.to_owned(), 15)];
        assert!(matches!(
            governor.book_windows(&demand).unwrap(),
            Booking::Booked(_)
        ));
        // 15 of 60 requests per minute: the next question may start about 15 s later.
        match governor.book_windows(&demand).unwrap() {
            Booking::Wait(wait) => {
                assert!(wait > Duration::from_secs(14) && wait <= Duration::from_secs(15))
            }
            other => panic!("expected pacing, got {other:?}"),
        }
    }

    #[test]
    fn abandoned_bookings_expire_with_their_window() {
        let governor = Governor::local();
        let scope = "example.org/search";
        governor
            .observe_source(scope, &window(3, 3, now_ms() + 30), Duration::from_secs(10))
            .unwrap();
        // Book the whole window and never send.
        assert!(matches!(
            governor.book_windows(&[(scope.to_owned(), 3)]).unwrap(),
            Booking::Booked(_)
        ));
        assert!(governor.source_ticket(scope, None).unwrap().is_some());
        std::thread::sleep(Duration::from_millis(50));
        // The next window starts with nothing used.
        assert!(governor.source_ticket(scope, None).unwrap().is_none());
    }

    #[test]
    fn a_damaged_state_file_is_set_aside_with_an_error_and_writes_replace_it_whole() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("state.json"), b"{not json").unwrap();
        let governor = Governor::at(dir.path()).unwrap();
        let provider = budget("a", 600.0);
        assert!(governor.consume(&provider).is_err());
        assert!(governor.consume(&provider).unwrap());
        let names: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert!(names.iter().any(|n| n.starts_with("state.json.damaged-")));
        assert!(!names
            .iter()
            .any(|n| n.starts_with("state.json.") && !n.contains("damaged")));
        let saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("state.json")).unwrap()).unwrap();
        assert!(saved["providers"]["a"]["tokens"].as_f64().unwrap() < 100.0);
    }

    #[test]
    fn an_unusable_state_folder_is_an_error_not_a_silent_local_state() {
        let dir = tempfile::tempdir().unwrap();
        let governor = Governor::at(&dir.path().join("host")).unwrap();
        std::fs::remove_dir_all(dir.path().join("host")).unwrap();
        std::fs::write(dir.path().join("host"), b"a file, not a folder").unwrap();
        assert!(governor.consume(&budget("a", 600.0)).is_err());
    }
}
