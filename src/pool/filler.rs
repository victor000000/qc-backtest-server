//! Background task that fills the warm pool using parallel warmers.

use std::sync::Arc;

use tokio::time::{Duration, sleep};

use super::WarmPool;
use crate::client::QcClient;
use crate::config::StrategyConfig;
use crate::db::Db;
use crate::rate_limit::RateLimitState;
use crate::runner::ProjectLock;

/// Bundled parameters for the warm pool filler task.
pub struct WarmPoolFillerParams {
    pub pool: Arc<WarmPool>,
    pub client: Arc<QcClient>,
    pub dbs: Vec<Db>,
    pub strategies: Vec<StrategyConfig>,
    pub project_lock: ProjectLock,
    pub name_prefix: Option<String>,
    pub rl_state: Option<RateLimitState>,
    pub slot_reserve: usize,
}

/// Background task that fills the warm pool using parallel warmers.
/// Concurrency adapts to available capacity: min(room, `free_projects`).
///
/// `slot_reserve` projects are always kept unlocked so slots that fall
/// back to direct-claim can always get a project. Without this, a full
/// warm pool could lock every project, starving direct-claim slots.
pub async fn warm_pool_filler(params: WarmPoolFillerParams) {
    let WarmPoolFillerParams {
        pool,
        client,
        dbs,
        strategies,
        project_lock,
        name_prefix,
        rl_state,
        slot_reserve,
    } = params;
    let num_projects = strategies.first().map_or(0, |s| s.project_ids.len());
    let mut fill_counter: usize = 0;

    loop {
        // Adaptive concurrency: use free projects, not a fixed cap.
        // Always reserve `slot_reserve` projects for slot direct-claims so
        // a slot that falls through never hits "all projects locked".
        let locked_count = project_lock.lock().await.len();
        let free_total = num_projects.saturating_sub(locked_count);
        let free_for_warm = free_total.saturating_sub(slot_reserve);

        let pool_len = pool.entries.lock().await.len();
        let room = if pool_len >= pool.max_entries || free_for_warm == 0 {
            0
        } else {
            (pool.max_entries - pool_len).min(free_for_warm)
        };

        if room == 0 {
            tracing::trace!(
                "warm-pool: throttled (pool={}/{}, free_total={free_total}, \
                 free_for_warm={free_for_warm}, reserve={slot_reserve})",
                pool_len,
                pool.max_entries
            );
            // Shorter throttle — slots drain pool in seconds; need to react fast.
            sleep(Duration::from_millis(105)).await;
            continue;
        }

        tracing::debug!(
            "warm-pool: filling {room} slots \
             (pool={}/{}, free_for_warm={free_for_warm}, reserve={slot_reserve})",
            pool.len().await,
            pool.max_entries
        );

        // Spawn parallel warmers
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..room {
            let c = Arc::clone(&client);
            let d = dbs.clone();
            let s = strategies.clone();
            let pl = Arc::clone(&project_lock);
            let np = name_prefix.clone();
            let rl = rl_state.clone();
            let fc = fill_counter;
            fill_counter += 1;

            set.spawn(async move {
                super::job::warm_one_job(super::job::PoolJobParams {
                    client: &c,
                    dbs: &d,
                    strategies: &s,
                    project_lock: &pl,
                    counter: fc,
                    name_prefix: &np,
                    rl_state: &rl,
                })
                .await
            });
        }

        // Stream results as they complete
        let mut warmed = 0;
        while let Some(result) = set.join_next().await {
            if let Ok(Some(entry)) = result {
                tracing::debug!(
                    "warm-pool: added {}/{} (project={})",
                    entry.strategy,
                    entry.job.name,
                    entry.project_id,
                );
                pool.push(entry).await;
                warmed += 1;
            }
        }

        if warmed > 0 {
            tracing::debug!(
                "warm-pool: batch done, warmed {warmed}, \
                 pool={}/{}",
                pool.len().await,
                pool.max_entries
            );
        }

        // Short pause between batches — keeps warmer busy when slots drain pool.
        sleep(Duration::from_millis(70)).await;
    }
}
