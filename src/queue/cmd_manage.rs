//! Queue management: retry failed jobs and clean old entries.

use std::path::Path;

use anyhow::Result;

use crate::db::Db;

use super::{open_db, strategy_targets};

/// Retry failed jobs by resetting them to queued.
///
/// # Errors
/// Returns `Err` if neither `name` nor `batch` is provided, or on SQL error.
pub async fn retry(
    data_dir: &Path,
    strategy: &str,
    name: Option<&str>,
    batch: Option<&str>,
) -> Result<()> {
    if name.is_none() && batch.is_none() {
        anyhow::bail!("must specify --name or --batch (or both)");
    }

    let db = open_db(data_dir, strategy)?;
    let n = name.map(std::string::ToString::to_string);
    let b = batch.map(std::string::ToString::to_string);

    let changed: usize = db
        .call(move |conn| {
            let c = match (&n, &b) {
                (Some(name), _) => conn.execute(
                    "UPDATE backtest_queue \
                     SET status='queued', error_message=NULL, \
                         retry_count=0, result_json=NULL
                     WHERE name=?1 AND status='failed'",
                    [name],
                )?,
                (None, Some(batch)) => conn.execute(
                    "UPDATE backtest_queue \
                     SET status='queued', error_message=NULL, \
                         retry_count=0, result_json=NULL
                     WHERE batch=?1 AND status='failed'",
                    [batch],
                )?,
                _ => 0,
            };
            Ok(c)
        })
        .await?;

    let scope = match (name, batch) {
        (Some(n), _) => format!("{strategy}/{n}"),
        (_, Some(b)) => format!("{strategy} batch={b}"),
        _ => strategy.to_string(),
    };
    if changed > 0 {
        println!("retried {changed} failed job(s) in {scope} -> queued");
    } else {
        println!("{scope}: no failed jobs to retry");
    }
    Ok(())
}

/// Remove old done/failed/cancelled entries from the queue table.
///
/// # Errors
/// Returns `Err` on SQL execution error.
pub async fn clean(
    data_dir: &Path,
    strategy: Option<&str>,
    older_than_days: i64,
    include_cancelled: bool,
    dry_run: bool,
) -> Result<()> {
    let targets = strategy_targets(data_dir, strategy)?;

    for (strat, path) in &targets {
        if !path.exists() {
            continue;
        }
        let db = Db::open(path, strat)?;
        let days = older_than_days;
        let with_cancelled = include_cancelled;

        let (count, deleted): (i64, usize) = db
            .call(move |conn| {
                let statuses = if with_cancelled {
                    "('done','failed','cancelled')"
                } else {
                    "('done','failed')"
                };
                let where_clause = format!(
                    "status IN {statuses} \
                     AND completed_at < \
                     datetime('now', '-{days} days')"
                );

                let count: i64 = conn.query_row(
                    &format!(
                        "SELECT COUNT(*) FROM backtest_queue \
                         WHERE {where_clause}"
                    ),
                    [],
                    |r| r.get(0),
                )?;

                let deleted = if dry_run {
                    0
                } else {
                    conn.execute(
                        &format!(
                            "DELETE FROM backtest_queue \
                             WHERE {where_clause}"
                        ),
                        [],
                    )?
                };

                Ok((count, deleted))
            })
            .await?;

        if dry_run {
            println!(
                "{strat}: would delete {count} entries \
                 (older than {older_than_days}d)"
            );
        } else {
            println!("{strat}: deleted {deleted} entries");
        }
    }
    Ok(())
}
