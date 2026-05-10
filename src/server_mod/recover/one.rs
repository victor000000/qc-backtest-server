//! Per-job recovery work spawned in parallel by `recover_running_jobs`.

use crate::client::QcClient;
use crate::db::{Db, QueueJob};
use crate::runner::collect_parse::parse_and_store;

pub(super) async fn recover_one_job(
    client: &QcClient,
    db: &Db,
    job_id: i64,
    name: String,
    backtest_id: Option<String>,
    project_id: Option<i64>,
) {
    let mut marked_done = false;

    if let (Some(bt_id), Some(pid)) = (&backtest_id, project_id)
        && !bt_id.is_empty()
    {
        match client.read_backtest(pid, bt_id).await {
            Ok(resp) => {
                let (completed, stats_ready) =
                    resp.backtest.as_ref().map_or((false, false), |bt| {
                        (bt.completed, !bt.statistics.is_empty())
                    });

                if completed && stats_ready {
                    let job_row = fetch_job_row(db, job_id).await;
                    if let Some(job) = job_row
                        && parse_and_store(&job, db, pid, bt_id, 0.0, &resp)
                            .await
                            .is_ok()
                    {
                        tracing::info!("  {name}: completed with stats — collected + done");
                        marked_done = true;
                    }
                } else {
                    tracing::info!(
                        "  {name}: orphan on QC (completed={completed}, \
                         stats={stats_ready}) — cancelling + requeuing to free node"
                    );
                }
            }
            Err(e) => {
                tracing::warn!("  {name}: couldn't read QC state: {e} — cancelling + requeuing");
            }
        }

        if !marked_done && let Err(e) = client.delete_backtest(pid, bt_id).await {
            tracing::warn!("  {name}: couldn't cancel QC backtest: {e}");
        }
    }

    if marked_done {
        return;
    }

    requeue_job(db, job_id).await;
    tracing::info!("  {name}: requeued");
}

async fn fetch_job_row(db: &Db, job_id: i64) -> Option<QueueJob> {
    let strategy_name = db.strategy.clone();
    db.call(move |conn| {
        let row = conn.query_row(
            "SELECT id, name, code, description, hypothesis,
                    based_on, batch, priority
             FROM backtest_queue WHERE id=?1",
            [job_id],
            |r| {
                Ok(QueueJob {
                    id: r.get(0)?,
                    name: r.get(1)?,
                    code: r.get(2)?,
                    description: r.get(3)?,
                    hypothesis: r.get(4)?,
                    based_on: r.get(5)?,
                    batch: r.get(6)?,
                    priority: r.get(7)?,
                    strategy: strategy_name.clone(),
                })
            },
        )?;
        Ok(Some(row))
    })
    .await
    .unwrap_or(None)
}

async fn requeue_job(db: &Db, job_id: i64) {
    let _ = db
        .call(move |conn| {
            conn.execute(
                "UPDATE backtest_queue
                 SET status='queued', started_at=NULL,
                     node_id=NULL, backtest_id=NULL,
                     compile_id=NULL, project_id=NULL
                 WHERE id=?1",
                [job_id],
            )?;
            Ok(())
        })
        .await;
}
