//! Single job warming: find work, lock project, push code, compile.
//!
//! Sub-modules:
//! - `claim`: `strategy_has_work`, `lock_free_project`, `claim_job`
//! - `job_steps`: push + compile + poll steps
//! - `push`: `push_code`, `compile_code`, `poll_compile`

use std::time::Instant;

use super::WarmEntry;
use super::claim::{ClaimedJob, claim_job, lock_free_project, strategy_has_work};
use super::job_steps::execute_warm_steps;
use crate::client::QcClient;
use crate::config::StrategyConfig;
use crate::db::Db;
use crate::rate_limit::RateLimitState;
use crate::runner::ProjectLock;

/// Bundled parameters for warming a single job.
pub(crate) struct PoolJobParams<'a> {
    pub client: &'a QcClient,
    pub dbs: &'a [Db],
    pub strategies: &'a [StrategyConfig],
    pub project_lock: &'a ProjectLock,
    pub counter: usize,
    pub name_prefix: &'a Option<String>,
    pub rl_state: &'a Option<RateLimitState>,
}

/// Warm a single job: find queued work, lock a project, push+compile.
///
/// Returns `Some(WarmEntry)` on success, `None` if no work or no free projects.
pub(crate) async fn warm_one_job(params: PoolJobParams<'_>) -> Option<WarmEntry> {
    let PoolJobParams {
        client,
        dbs,
        strategies,
        project_lock,
        counter,
        name_prefix,
        rl_state,
    } = params;
    let warm_start = Instant::now();

    // Strategy ordering: rotate by counter so over many calls each strategy
    // gets equal "first-pick" attempts. (Previous shuffle was deterministic
    // and only alternated for 2 strategies; this generalizes.)
    let n = dbs.len();
    let mut strat_indices: Vec<usize> = (0..n).collect();
    if n > 0 {
        let offset = counter % n;
        strat_indices.rotate_left(offset);
    }

    for &si in &strat_indices {
        let db = &dbs[si];

        // 1. Check if strategy has queued work
        if !strategy_has_work(db, name_prefix.as_deref()).await {
            tracing::trace!(
                "warm-pool: strategy[{si}]={} has no work",
                strategies[si].name
            );
            continue;
        }

        tracing::debug!(
            "warm-pool: strategy[{si}]={} has work, finding project",
            strategies[si].name
        );

        // 2. Find and lock a free project
        let Some(project_id) =
            lock_free_project(project_lock, &strategies[si].project_ids, counter).await
        else {
            tracing::debug!("warm-pool: no free projects for {}", strategies[si].name);
            continue;
        };

        // 3. Claim a job from the queue
        let Some((job, project_id)) = claim_job(
            db,
            project_lock,
            project_id,
            name_prefix.as_deref(),
            &strategies[si].project_ids,
            counter,
        )
        .await
        else {
            continue;
        };

        // 4-7. Push, compile, poll
        let claimed = ClaimedJob { job, project_id };
        if let Some(entry) = execute_warm_steps(
            client,
            rl_state.as_ref(),
            project_lock,
            db,
            claimed,
            &strategies[si],
            warm_start,
        )
        .await
        {
            return Some(entry);
        }
    }

    tracing::debug!(
        "warm-pool: no work or no free projects (took {}ms)",
        warm_start.elapsed().as_millis()
    );
    None
}
