//! Queue listing subcommand.

use std::path::Path;

use anyhow::Result;

use crate::db::Db;

use super::{QueueRow, strategy_targets, truncate_name};

/// List jobs in the queue with optional filters.
///
/// # Errors
/// Returns `Err` on SQL execution error or if a strategy database fails to open.
pub async fn list(
    data_dir: &Path,
    strategy: Option<&str>,
    status_filter: &str,
    batch_filter: Option<&str>,
    name_like: Option<&str>,
    limit: usize,
) -> Result<()> {
    let statuses: Vec<String> = status_filter
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    let targets = strategy_targets(data_dir, strategy)?;

    println!(
        "{:<6} {:<35} {:<10} {:<6} {:<12} SUBMITTED",
        "STRAT", "NAME", "STATUS", "PRI", "BATCH"
    );
    println!("{}", "-".repeat(90));

    for (strat, path) in &targets {
        if !path.exists() {
            continue;
        }
        let db = Db::open(path, strat)?;

        // Build dynamic SQL
        let mut conditions = vec!["1=1".to_string()];
        let mut params_vec: Vec<String> = Vec::new();

        // Status filter
        let placeholders: Vec<String> = statuses
            .iter()
            .enumerate()
            .map(|(i, _)| format!("?{}", i + 1))
            .collect();
        conditions.push(format!("status IN ({})", placeholders.join(",")));
        for s in &statuses {
            params_vec.push(s.clone());
        }

        // Batch filter
        if let Some(b) = batch_filter {
            params_vec.push(b.to_string());
            conditions.push(format!("batch=?{}", params_vec.len()));
        }

        // Name pattern filter
        if let Some(pat) = name_like {
            params_vec.push(pat.to_string());
            conditions.push(format!("name LIKE ?{}", params_vec.len()));
        }

        params_vec.push((i64::try_from(limit).unwrap_or(i64::MAX)).to_string());
        let limit_idx = params_vec.len();

        let where_clause = conditions.join(" AND ");
        let sql = format!(
            "SELECT name, status, priority, batch, submitted_at \
             FROM backtest_queue
             WHERE {where_clause}
             ORDER BY priority ASC, id DESC LIMIT ?{limit_idx}"
        );

        let rows: Vec<QueueRow> = db
            .call(move |conn| {
                let mut stmt = conn.prepare(&sql)?;
                let params_ref: Vec<Box<dyn rusqlite::types::ToSql>> = params_vec
                    .iter()
                    .map(|s| -> Box<dyn rusqlite::types::ToSql> { Box::new(s.clone()) })
                    .collect();
                let refs: Vec<&dyn rusqlite::types::ToSql> =
                    params_ref.iter().map(std::convert::AsRef::as_ref).collect();
                let rows = stmt
                    .query_map(refs.as_slice(), |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .await?;

        for (name, status, pri, batch, submitted) in &rows {
            println!(
                "{:<6} {:<35} {:<10} {:<6} {:<12} {}",
                strat,
                truncate_name(name, 35),
                status,
                pri,
                batch.as_deref().unwrap_or("-"),
                submitted.as_deref().unwrap_or("-"),
            );
        }
    }
    Ok(())
}
