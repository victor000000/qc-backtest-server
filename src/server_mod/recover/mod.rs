//! Recovery of jobs stuck in 'running' state from a previous crash.
//!
//! Behavior per row (fast, bounded, parallel):
//!   - completed on QC WITH stats already populated → parse + mark done
//!     (single DB call, no retries — avoids hanging startup)
//!   - any other orphan (in-flight, completed-no-stats, QC error) →
//!     cancel on QC + requeue to free the node
//!
//! Runs all per-orphan work in parallel so a slow QC API call (up to ~6min
//! in the `with_retry` ladder) doesn't stack into a server-startup stall.

mod one;

use std::sync::Arc;

use anyhow::Result;

use crate::client::QcClient;
use crate::db::Db;

use self::one::recover_one_job;

pub(crate) async fn recover_running_jobs(dbs: &[Db], client: &Arc<QcClient>) -> Result<()> {
    for db in dbs {
        let running: Vec<(i64, String, Option<String>, Option<i64>)> = db
            .call(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, name, backtest_id, project_id \
                     FROM backtest_queue WHERE status='running'",
                )?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .await?;

        if running.is_empty() {
            continue;
        }

        tracing::info!(
            "{}: recovering {} stale running jobs (parallel)",
            db.strategy,
            running.len()
        );

        let mut set = tokio::task::JoinSet::new();
        for (job_id, name, backtest_id, project_id) in running {
            let db_c = db.clone();
            let cli_c = Arc::clone(client);
            set.spawn(async move {
                recover_one_job(&cli_c, &db_c, job_id, name, backtest_id, project_id).await;
            });
        }
        while set.join_next().await.is_some() {}
    }
    Ok(())
}
