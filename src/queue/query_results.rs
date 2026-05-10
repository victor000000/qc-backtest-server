//! Query subcommands: errors listing and top experiments.

use std::path::Path;

use anyhow::Result;

use super::{open_db, truncate_name};

/// Show recent errors with QC API messages.
///
/// # Errors
/// Returns `Err` on SQL execution error or if the strategy database fails to open.
pub async fn errors(
    data_dir: &Path,
    strategy: &str,
    limit: usize,
    detail_name: Option<&str>,
) -> Result<()> {
    let db = open_db(data_dir, strategy)?;

    // If --detail is given, show full result_json for that job
    if let Some(name) = detail_name {
        let n = name.to_string();
        let row: Option<(String, Option<String>, Option<String>)> = db
            .call(move |conn| {
                conn.query_row(
                    "SELECT status, error_message, result_json \
                     FROM backtest_queue WHERE name=?1",
                    [&n],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map(Some)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    _ => Err(e.into()),
                })
            })
            .await?;

        match row {
            Some((status, err, json)) => {
                println!("{strategy}/{name}  status={status}");
                if let Some(e) = &err {
                    println!("\nerror_message:\n  {e}");
                }
                if let Some(j) = &json {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(j) {
                        println!("\nresult_json:");
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&v).unwrap_or_else(|_| j.clone())
                        );
                    } else {
                        println!("\nresult_json (raw):\n{j}");
                    }
                } else {
                    println!("\n(no result_json saved)");
                }
            }
            None => println!("{strategy}/{name}: not found"),
        }
        return Ok(());
    }

    // Otherwise list recent failures
    let lim = limit;
    let rows: Vec<(String, String, Option<String>, Option<String>)> = db
        .call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT name, status, error_message, completed_at \
                 FROM backtest_queue
                 WHERE status='failed'
                 ORDER BY completed_at DESC LIMIT ?1",
            )?;
            let rows = stmt
                .query_map([i64::try_from(lim).unwrap_or(i64::MAX)], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await?;

    if rows.is_empty() {
        println!("{strategy}: no failures");
        return Ok(());
    }

    println!("{strategy} recent failures:");
    println!("{:<35} {:<22} ERROR", "NAME", "COMPLETED");
    println!("{}", "-".repeat(100));
    for (name, _status, err, completed) in &rows {
        let err_short = err.as_deref().unwrap_or("");
        let err_display = if err_short.len() > 60 {
            &err_short[..60]
        } else {
            err_short
        };
        println!(
            "{:<35} {:<22} {}",
            truncate_name(name, 35),
            completed.as_deref().unwrap_or("-"),
            err_display,
        );
    }
    println!(
        "\nUse --detail <name> to see full QC API response \
         for a specific failure."
    );
    Ok(())
}

// top() is in query_top.rs
