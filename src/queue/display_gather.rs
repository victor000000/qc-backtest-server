//! Status display helpers: gather strategy stats.

use anyhow::Result;

use crate::db::Db;

use super::display_fmt::fmt_int;

fn count_status(counts: &[(String, i64)], status: &str) -> i64 {
    counts
        .iter()
        .find(|(s, _)| s == status)
        .map_or(0, |(_, c)| *c)
}

async fn experiment_count(db: &Db) -> i64 {
    db.call(|conn| {
        conn.query_row(
            "SELECT COUNT(*) FROM experiments WHERE status='success'",
            [],
            |r| r.get(0),
        )
        .map_err(Into::into)
    })
    .await
    .unwrap_or(0)
}

async fn session_ok_fail(db: &Db, uptime_secs: i64) -> (i64, i64) {
    db.call(move |conn| {
        let ok: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM backtest_queue \
                 WHERE status='done' \
                 AND completed_at > datetime('now', ?1 || ' seconds')",
                [format!("-{uptime_secs}")],
                |r| r.get(0),
            )
            .unwrap_or(0);
        let fail: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM backtest_queue \
                 WHERE status='failed' \
                 AND completed_at > datetime('now', ?1 || ' seconds')",
                [format!("-{uptime_secs}")],
                |r| r.get(0),
            )
            .unwrap_or(0);
        Ok((ok, fail))
    })
    .await
    .unwrap_or((0, 0))
}

fn done_pct(done: i64, total: i64) -> f64 {
    if total <= 0 {
        return 0.0;
    }
    let done_f = f64::from(u32::try_from(done).unwrap_or(u32::MAX));
    let total_f = f64::from(u32::try_from(total).unwrap_or(u32::MAX));
    done_f / total_f * 100.0
}

async fn bt_running_count(db: &Db) -> i64 {
    db.call(|conn| {
        conn.query_row(
            "SELECT COUNT(*) FROM backtest_queue \
             WHERE status='running' AND backtest_id IS NOT NULL",
            [],
            |r| r.get(0),
        )
        .map_err(Into::into)
    })
    .await
    .unwrap_or(0)
}

async fn recent_failures(db: &Db) -> Vec<(String, String)> {
    db.call(|conn| {
        let mut stmt = conn.prepare(
            "SELECT name, COALESCE(error_message, '') FROM backtest_queue \
             WHERE status='failed' \
             ORDER BY completed_at DESC LIMIT 4",
        )?;
        let rows = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    })
    .await
    .unwrap_or_default()
}

/// Gather per-strategy queue counts, session stats, and recent failures.
pub(super) async fn gather_strategy_status(
    db: &Db,
    strategy: &str,
    uptime_secs: i64,
) -> Result<(i64, i64, String, Vec<(String, String)>)> {
    let counts = db.queue_counts().await?;
    let queued = count_status(&counts, "queued");
    let running = count_status(&counts, "running");
    let done = count_status(&counts, "done");
    let failed = count_status(&counts, "failed");

    let exp_count = experiment_count(db).await;
    let (s_ok, s_fail) = session_ok_fail(db, uptime_secs).await;
    let pct = done_pct(done, done + failed);
    let bt_running = bt_running_count(db).await;

    let r_display = if bt_running < running {
        format!("{bt_running}/{running}")
    } else {
        format!("{running}")
    };
    let line = format!(
        "\u{2502}  {strategy:<5}  queued {queued:>4}  running {r_display:<6}  \
         done {done_s:>9}  failed {failed_s:>5}  experiments {exp_s:>8}  {pct:>5.1}%",
        done_s = fmt_int(done),
        failed_s = fmt_int(failed),
        exp_s = fmt_int(exp_count),
    );

    let recent_fails = recent_failures(db).await;
    Ok((s_ok, s_fail, line, recent_fails))
}

// compute_overhead_pct is in display_overhead.rs
// print_status_box is in display_box.rs
