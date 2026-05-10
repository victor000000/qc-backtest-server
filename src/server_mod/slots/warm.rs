//! Warm pool + create-semaphore setup for slot launching.
//!
//! Hosts the rationale-heavy comments for sizing decisions
//! (`slot_reserve`, `max_warm`, `create_semaphore` permits).

use std::sync::Arc;

use tokio::task::JoinHandle;

use crate::client::QcClient;
use crate::config::ServerConfig;
use crate::db::Db;
use crate::rate_limit::RateLimitState;

/// Warm pool plumbing returned by [`build_warm_pool`].
pub(super) struct WarmPoolBundle {
    pub pool: Arc<crate::pool::WarmPool>,
    pub handle: JoinHandle<()>,
    pub create_semaphore: Arc<tokio::sync::Semaphore>,
}

/// Build the warm pool, spawn its filler, and create the global
/// `create_backtest` semaphore.
///
/// Sizing rationale (preserved from inline history):
/// - `max_warm = (num_projects - num_slots).max(num_slots) * 2` keeps the pool
///   large enough that nodes never starve for pre-compiled work.
/// - `slot_reserve = (num_slots / 3).max(3)` keeps a few projects unlocked so
///   a slot that falls through to direct-claim can always grab one. Tuned
///   through R1359 throughput exploration; lower values caused
///   "all projects locked" cascades, higher values shrank the pool.
/// - `create_semaphore = 6` permits — serializes burst-create calls to avoid
///   QC rate-limit penalty (observed 18s `create_api` when 5 slots burst-created
///   simultaneously after a 6-bt completion wave). R1359 bumped 4 -> 6 to ease
///   creation throttle while backpressure (2x slots) remains the QC-side
///   governor.
pub(super) fn build_warm_pool(
    config: &ServerConfig,
    dbs: &[Db],
    client: &Arc<QcClient>,
    num_slots: usize,
    rl_state: &RateLimitState,
    project_lock: &crate::runner::ProjectLock,
) -> WarmPoolBundle {
    let num_projects = config
        .strategies
        .first()
        .map_or(num_slots, |s| s.project_ids.len());
    let max_warm = (num_projects.saturating_sub(num_slots).max(num_slots)) * 2;
    let pool = Arc::new(crate::pool::WarmPool::new(
        Arc::clone(project_lock),
        max_warm,
    ));
    let slot_reserve = (num_slots / 3).max(3);
    let handle = tokio::spawn(crate::pool::warm_pool_filler(
        crate::pool::WarmPoolFillerParams {
            pool: Arc::clone(&pool),
            client: Arc::clone(client),
            dbs: dbs.to_vec(),
            strategies: config.strategies.clone(),
            project_lock: Arc::clone(project_lock),
            name_prefix: None,
            rl_state: Some(rl_state.clone()),
            slot_reserve,
        },
    ));
    tracing::info!("warm pool slot_reserve = {slot_reserve}");
    tracing::info!(
        "warm pool started (max {max_warm} entries, \
         adaptive concurrency up to {} warmers)",
        num_projects.saturating_sub(num_slots)
    );

    // 2026-05-07 PM: reverted 10 → 6 because slots holding permits across the
    // full retry chain (≤24s) cascaded into 33s sem_wait queues. The
    // sem-rotate fix (commit 5dbafa3, drops permit between retries) removed
    // that cascade — restart16 saw max sem_wait 481ms vs 33000ms before.
    // With cascade gone, raising back to num_slots is safe: storms now
    // self-heal because no slot blocks the queue, and more concurrent
    // permits cuts the steady-state sem_wait (restart19 with 6 had p99
    // sem_wait 8287ms — bottleneck shifted from cascade to capacity).
    let create_permits = num_slots;
    let create_semaphore = Arc::new(tokio::sync::Semaphore::new(create_permits));
    tracing::info!("create_backtest semaphore: {create_permits} permits (rate-limit dodge)");

    WarmPoolBundle {
        pool,
        handle,
        create_semaphore,
    }
}
