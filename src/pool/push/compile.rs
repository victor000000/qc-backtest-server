//! Warm-pool compile + compile-poll helpers.

use std::time::Instant;

use tokio::time::{Duration, sleep};

use crate::client::QcClient;
use crate::db::{Db, QueueJob};
use crate::pool::claim::requeue_job;
use crate::rate_limit::{RateLimitState, is_rate_limit, retry_on_rate_limit};

pub(crate) async fn compile_code(
    client: &QcClient,
    rl_state: Option<&RateLimitState>,
    project_id: i64,
    db: &Db,
    job: &QueueJob,
    job_name: &str,
) -> Option<String> {
    let compile_result =
        retry_on_rate_limit(rl_state, 3, || client.create_compile(project_id)).await;

    match compile_result {
        Ok(c) if c.success => {
            tracing::debug!(
                "warm-pool: compile created for {job_name}: {}",
                c.compile_id
            );
            Some(c.compile_id)
        }
        Ok(c) => {
            tracing::warn!(
                "warm-pool: compile create failed for {job_name}: {:?}",
                c.errors
            );
            let _ = db
                .mark_failed(job.id, &format!("warmpool_compile: {:?}", c.errors), None)
                .await;
            None
        }
        Err(e) => {
            tracing::warn!("warm-pool: compile error for {job_name}: {e}");
            requeue_job(db, job.id).await;
            None
        }
    }
}

pub(crate) async fn poll_compile(
    client: &QcClient,
    project_id: i64,
    compile_id: &str,
    db: &Db,
    job: &QueueJob,
    job_name: &str,
) -> bool {
    let deadline = Instant::now() + Duration::from_mins(1);

    loop {
        sleep(Duration::from_millis(1050)).await;

        let r = match client.read_compile(project_id, compile_id).await {
            Ok(r) => r,
            Err(e) if is_rate_limit(&e) => {
                sleep(Duration::from_millis(1400)).await;
                continue;
            }
            Err(e) => {
                tracing::warn!("warm-pool: compile poll error for {job_name}: {e}");
                requeue_job(db, job.id).await;
                return false;
            }
        };

        match r.state.as_str() {
            "BuildSuccess" => {
                tracing::debug!("warm-pool: compile OK for {job_name}");
                return true;
            }
            "BuildError" => {
                tracing::debug!("warm-pool: compile error for {job_name}: {:?}", r.logs);
                let _ = db
                    .mark_failed(
                        job.id,
                        &format!("warmpool_compile_error: {:?}", r.logs),
                        None,
                    )
                    .await;
                return false;
            }
            "InQueue" if Instant::now() > deadline => {
                tracing::warn!("warm-pool: compile timeout for {job_name}");
                requeue_job(db, job.id).await;
                return false;
            }
            _ => {}
        }
    }
}
