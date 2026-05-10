use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::Result;
use tokio::time::{Duration, sleep};

use crate::db::Db;

/// Check if an error looks like a rate limit (HTTP 429 or "rate limit" in message).
pub(crate) fn is_rate_limit(e: &anyhow::Error) -> bool {
    let msg = format!("{e:?}").to_lowercase();
    msg.contains("429")
        || msg.contains("rate limit")
        || msg.contains("rate_limit")
        || msg.contains("too many")
        || msg.contains("slow down")
}

/// Shared adaptive rate-limit state for the real server.
/// Persisted to the first strategy DB on update so restarts don't start cold.
#[derive(Clone)]
pub struct RateLimitState {
    /// Learned backoff delay in ms (exponential moving average).
    pub estimated_delay_ms: Arc<std::sync::atomic::AtomicU64>,
    /// Earliest instant any slot is allowed to fire its NEXT
    /// `create_backtest`. Acts as a global pacer to convert bursts into a
    /// trickle. Bumped forward by the spacing on each successful gate, and
    /// pushed further forward when a rate-limit error is observed (so the
    /// entire fleet backs off, not just the failing slot).
    pub create_pacer_until: Arc<Mutex<Instant>>,
}

impl RateLimitState {
    /// Load from DB or use default.
    pub async fn load(db: &Db) -> Self {
        let saved = db
            .get_setting("server_state", "rate_limit_delay_ms")
            .await
            .ok()
            .flatten()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(1000); // default 1s
        tracing::info!("rate limit backoff loaded: {saved}ms");
        Self {
            estimated_delay_ms: Arc::new(std::sync::atomic::AtomicU64::new(saved)),
            create_pacer_until: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// Spacing between consecutive `create_backtest` attempts. Restart9
    /// (`sem`=10, no pacer) hit 19.5/min with 151 `prep_failed`/min — slots
    /// bailed-and-retried fast. Restart12 (`sem`=6, 350ms pacer) dropped
    /// to 10.5/min because the pacer serialized submission. With the
    /// sem-rotate fix (commit 5dbafa3) preventing 33s `sem_wait` cascades
    /// during storms, the pacer's role narrows to controlling RATE only.
    /// 2026-05-08: 30ms→20ms per user direction. With backpressure caps
    /// loosened (12× hard, 8× soft) and `sem=num_slots`, tighter spacing
    /// keeps pacer non-binding for steady-state submission. 20ms = 50
    /// req/s burst tolerance experiment.
    /// 2026-05-09: 20ms→10ms per user direction.
    /// 2026-05-10: ×0.7 → 7ms (sleep-budget pass).
    const CREATE_MIN_SPACING_MS: u64 = 7;
    /// On a rate-limit hit, push the global pacer forward this far so the
    /// whole fleet pauses instead of retry-storming.
    const CREATE_BACKOFF_ON_HIT_MS: u64 = 3_500;
    /// Hard cap on future wait above `now` enforced by the pacer. Without
    /// this, successive rate-limit hits each add `CREATE_BACKOFF_ON_HIT_MS`
    /// to the deadline — restart20 observed 42s pacer waits from ~8 hits
    /// in quick succession. 10s gives QC enough recovery time without
    /// stranding slots in long single-cycle waits.
    /// 2026-05-09: 10s→5s→2s→1s→1.5s per user direction.
    /// 2026-05-10: ×0.7 → 1.05s (sleep-budget pass).
    const CREATE_MAX_COOLDOWN_MS: u64 = 1_050;

    /// Atomically read+update the create-pacer slot. Returns the duration
    /// the caller must `sleep` before firing the API call. The slot moves
    /// forward by `CREATE_MIN_SPACING_MS` on every claim — even if the
    /// caller turns out to error.
    pub(crate) fn claim_create_slot(&self) -> Duration {
        let now = Instant::now();
        let spacing = Duration::from_millis(Self::CREATE_MIN_SPACING_MS);
        let mut guard = self
            .create_pacer_until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let earliest = *guard;
        let wait = earliest.saturating_duration_since(now);
        *guard = earliest.max(now) + spacing;
        wait
    }

    /// Push the global pacer forward when a rate-limit is observed, so
    /// every slot trying to create next must wait through the cooldown.
    /// Capped so that N rapid hits don't compound into 40+s waits — the
    /// pacer should never enforce more than `CREATE_MAX_COOLDOWN_MS` of
    /// future wait above `now`. Restart20 saw 42s pacer waits because
    /// successive hits each added 5s with no upper bound.
    pub(crate) fn record_rate_limit_hit(&self) {
        let backoff = Duration::from_millis(Self::CREATE_BACKOFF_ON_HIT_MS);
        let max_cooldown = Duration::from_millis(Self::CREATE_MAX_COOLDOWN_MS);
        let mut guard = self
            .create_pacer_until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        let new_until = (*guard).max(now) + backoff;
        let cap = now + max_cooldown;
        *guard = new_until.min(cap);
    }

    /// Save current state to DB.
    pub async fn save(&self, db: &Db) {
        let val = self
            .estimated_delay_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        let _ = db
            .set_setting("server_state", "rate_limit_delay_ms", &val.to_string())
            .await;
    }

    pub(crate) fn get_delay(&self) -> u64 {
        self.estimated_delay_ms
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Update estimate after a rate-limit + successful retry.
    pub(crate) fn record_wait(&self, actual_ms: u64) {
        let old = self
            .estimated_delay_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        // EMA: 70% old, 30% new
        let new = (old * 7 + actual_ms * 3) / 10;
        self.estimated_delay_ms
            .store(new.max(100), std::sync::atomic::Ordering::Relaxed);
    }
}

/// Retry an async operation on rate limit, using adaptive backoff from shared state.
/// If `rl_state` is None, uses fixed exponential backoff.
pub(crate) async fn retry_on_rate_limit<F, Fut, T>(
    rl_state: Option<&RateLimitState>,
    max_retries: usize,
    mut f: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut attempt = 0;
    loop {
        let start = Instant::now();
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if is_rate_limit(&e) && attempt < max_retries => {
                attempt += 1;
                let delay_ms = rl_state.map_or_else(
                    || 210 * (1 << attempt), // Fixed exponential: 0.42s, 0.84s, 1.68s
                    |rls| (rls.get_delay() * attempt as u64).max(350), // Adaptive: min 350ms
                );
                // Add jitter to avoid thundering herd (10 slots hitting simultaneously)
                let jitter = fastrand::u64(0..=(delay_ms / 3));
                let delay = Duration::from_millis((delay_ms + jitter).min(5_600));
                tracing::debug!(
                    target: "qc::api",
                    "qc_rate_limit_retry attempt={attempt}/{max_retries} delay_ms={} jitter_ms={jitter} cap_5_6s",
                    delay.as_millis()
                );
                sleep(delay).await;
                // Record actual wait when we eventually succeed
                if let Some(rls) = rl_state {
                    rls.record_wait(u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX));
                }
            }
            Err(e) => return Err(e),
        }
    }
}

/// Parse a percentage string like "12.34%" -> 12.34, or a dollar string like "$1,234.56" -> 1234.56.
pub(crate) fn parse_stat(s: &str) -> Option<f64> {
    let cleaned = s.replace(['$', '%', ',', ' '], "");
    cleaned.parse().ok()
}

/// Parse a QC statistics map value to f64.
pub(crate) fn stat_f64(
    stats: &std::collections::HashMap<String, String>,
    key: &str,
) -> Option<f64> {
    stats.get(key).and_then(|v| parse_stat(v))
}

pub(crate) fn stat_i64(
    stats: &std::collections::HashMap<String, String>,
    key: &str,
) -> Option<i64> {
    stats.get(key).and_then(|v| v.replace(',', "").parse().ok())
}
