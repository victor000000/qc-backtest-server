//! Database queries for the status report: strategy counts, overhead, recent failures.

use crate::db::Db;

use super::status_fmt::{RecentFailure, StrategyStatus};

/// Gather per-strategy queue counts, experiments, and recent failures.
pub(crate) async fn gather_status(db: &Db) -> Option<(StrategyStatus, Vec<RecentFailure>)> {
    let Ok(counts) = db.queue_counts().await else {
        return None;
    };
    let queued = counts
        .iter()
        .find(|(s, _)| s == "queued")
        .map_or(0, |(_, c)| *c);
    let running = counts
        .iter()
        .find(|(s, _)| s == "running")
        .map_or(0, |(_, c)| *c);
    // Split running into backtest-phase vs pre-warm
    let bt_running: i64 = db
        .call(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM backtest_queue \
                 WHERE status='running' \
                 AND backtest_id IS NOT NULL",
                [],
                |r| r.get(0),
            )
            .map_err(Into::into)
        })
        .await
        .unwrap_or(0);
    let done = counts
        .iter()
        .find(|(s, _)| s == "done")
        .map_or(0, |(_, c)| *c);
    let failed = counts
        .iter()
        .find(|(s, _)| s == "failed")
        .map_or(0, |(_, c)| *c);

    let exp_count: i64 = db
        .call(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM experiments \
                 WHERE status='success'",
                [],
                |r| r.get(0),
            )
            .map_err(Into::into)
        })
        .await
        .unwrap_or(0);

    let strat = StrategyStatus {
        name: db.strategy.clone(),
        queued,
        running,
        bt_running,
        done,
        failed,
        experiments: exp_count,
    };

    // Recent failures (last 5 minutes)
    let recent_fails: Vec<(String, String)> = db
        .call(|conn| {
            let mut stmt = conn.prepare(
                "SELECT name, error_message FROM backtest_queue
                 WHERE status='failed' \
                 AND completed_at > datetime('now', '-5 minutes')
                 ORDER BY completed_at DESC LIMIT 3",
            )?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
        .unwrap_or_default();

    let errors: Vec<RecentFailure> = recent_fails
        .into_iter()
        .map(|(name, error)| RecentFailure { name, error })
        .collect();

    Some((strat, errors))
}

/// Compute overhead % from recent completed jobs across all DBs.
pub(crate) async fn compute_overhead(dbs: &[Db], session_completed: u64) -> f64 {
    let mut total_wall = 0.0f64;
    let mut total_bt = 0.0f64;
    for db in dbs.iter().filter(|_| session_completed > 0) {
        let (wall, bt): (f64, f64) = db
            .call(|conn| {
                conn.query_row(
                    "SELECT
                        COALESCE(SUM(\
                            (julianday(completed_at)\
                             -julianday(started_at))*86400), 0),
                        COALESCE(SUM(runtime_seconds), 0)
                     FROM backtest_queue
                     WHERE status='done' AND runtime_seconds > 0
                       AND completed_at > \
                           datetime('now', '-10 minutes')",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .map_err(Into::into)
            })
            .await
            .unwrap_or((0.0, 0.0));
        total_wall += wall;
        total_bt += bt;
    }
    if total_wall > 0.0 {
        (total_wall - total_bt) / total_wall * 100.0
    } else {
        0.0
    }
}
