//! Warm job steps: push code, compile, poll compile.

use std::time::Instant;

use super::WarmEntry;
use super::claim::requeue_job;
use super::push::{compile_code, local_syntax_check, poll_compile, push_code};
use crate::client::QcClient;
use crate::config::StrategyConfig;
use crate::db::Db;
use crate::rate_limit::RateLimitState;
use crate::runner::ProjectLock;

use super::claim::ClaimedJob;

/// Execute push + compile + poll for a claimed job.
/// Returns `Some(WarmEntry)` on success, `None` on failure
/// (cleans up by unlocking project and requeuing).
pub(crate) async fn execute_warm_steps(
    client: &QcClient,
    rl_state: Option<&RateLimitState>,
    project_lock: &ProjectLock,
    db: &Db,
    claimed: ClaimedJob,
    strategy: &StrategyConfig,
    warm_start: Instant,
) -> Option<WarmEntry> {
    const QC_FILE_MAX: usize = 128_000;

    let job_name = claimed.job.name.clone();
    let project_id = claimed.project_id;

    // Safety check: verify project is locked
    {
        let locked = project_lock.lock().await;
        if !locked.contains(&project_id) {
            tracing::error!(
                "warm-pool: BUG -- project {project_id} not locked \
                 before push for {job_name}!"
            );
            requeue_job(db, claimed.job.id).await;
            return None;
        }
    }

    // Local size pre-check — QC rejects files > 128_000 chars. Pushing them
    // anyway burns API calls AND warmer requeues silently → infinite retry
    // loop (230+ retries observed today). Mark terminally failed.
    if claimed.job.code.len() > QC_FILE_MAX {
        let used = claimed.job.code.len();
        tracing::warn!(
            "warm-pool: {job_name} exceeds QC max file size \
             ({used} > {QC_FILE_MAX}), marking failed"
        );
        let _ = db
            .mark_failed(
                claimed.job.id,
                &format!(
                    "push_failed: [\"File exceeds the maximum size of \
                     {QC_FILE_MAX} characters by using {used}.\"]"
                ),
                None,
            )
            .await;
        project_lock.lock().await.remove(&project_id);
        return None;
    }

    // Local syntax check (catches IndentationError/SyntaxError before wasting a QC API call)
    if let Err(err) = local_syntax_check(&claimed.job.code, &job_name).await {
        tracing::warn!("warm-pool: local syntax check failed for {job_name}, marking failed");
        let _ = db
            .mark_failed(
                claimed.job.id,
                &format!(
                    "warmpool_compile_error: [\"local: {}\"]",
                    err.chars().take(200).collect::<String>()
                ),
                None,
            )
            .await;
        project_lock.lock().await.remove(&project_id);
        return None;
    }

    // Push code
    if let Err(()) = push_code(client, rl_state, project_id, &claimed.job.code, &job_name).await {
        project_lock.lock().await.remove(&project_id);
        requeue_job(db, claimed.job.id).await;
        return None;
    }

    // Compile
    let Some(compile_id) =
        compile_code(client, rl_state, project_id, db, &claimed.job, &job_name).await
    else {
        project_lock.lock().await.remove(&project_id);
        return None;
    };

    // Poll compile until done
    if !poll_compile(client, project_id, &compile_id, db, &claimed.job, &job_name).await {
        project_lock.lock().await.remove(&project_id);
        return None;
    }

    let warm_ms = warm_start.elapsed().as_millis();
    tracing::debug!(
        "warm-pool: warmed {}/{job_name} in {warm_ms}ms \
         on project {project_id}",
        strategy.name
    );

    Some(WarmEntry {
        job: claimed.job,
        db: db.clone(),
        project_id,
        compile_id,
        strategy: strategy.name.clone(),
        warmed_at: Instant::now(),
    })
}
