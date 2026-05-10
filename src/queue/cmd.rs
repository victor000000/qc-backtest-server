//! Queue enqueue command.
//!
//! Sub-modules:
//! - `cmd_cancel.rs`: cancel, `cancel_all`
//! - `cmd_manage.rs`: retry, clean

use std::path::Path;

use anyhow::Result;
use rusqlite::OptionalExtension;

use super::{code_hash, open_db};

/// Bundled input for the `enqueue` command.
pub struct QueueCmdInput<'a> {
    pub data_dir: &'a Path,
    pub strategy: &'a str,
    pub name: &'a str,
    pub code: &'a str,
    pub batch: Option<&'a str>,
    pub priority: i64,
    pub description: Option<&'a str>,
    pub hypothesis: Option<&'a str>,
    pub based_on: Option<&'a str>,
    pub project_id: Option<i64>,
}

/// Enqueue a backtest job.
///
/// # Errors
/// Returns `Err` on SQL execution error or if the job name already exists.
pub async fn enqueue(input: QueueCmdInput<'_>) -> Result<()> {
    let QueueCmdInput {
        data_dir,
        strategy,
        name,
        code,
        batch,
        priority,
        description,
        hypothesis,
        based_on,
        project_id,
    } = input;
    let db = open_db(data_dir, strategy)?;

    let name_log = name.to_string();
    let name = name.to_string();
    let hash = code_hash(code);
    let code = code.to_string();
    let batch = batch.map(std::string::ToString::to_string);
    let desc = description.map(std::string::ToString::to_string);
    let hyp = hypothesis.map(std::string::ToString::to_string);
    let based = based_on.map(std::string::ToString::to_string);

    let name_check = name.clone();
    let exists: Option<String> = db
        .call(move |conn| {
            conn.query_row(
                "SELECT status FROM backtest_queue WHERE name=?1",
                [&name_check],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
        })
        .await?;

    if let Some(status) = exists {
        anyhow::bail!(
            "{strategy}/{name_log} already exists in queue \
             (status={status}). Use a different name."
        );
    }

    // Check for duplicate code hash
    let hash_check = hash.clone();
    let dup_names: Vec<(String, String)> = db
        .call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT name, 'experiment' FROM experiments \
                 WHERE code_hash=?1
                 UNION ALL
                 SELECT name, 'queued' FROM backtest_queue \
                 WHERE code_hash=?1 \
                 AND status IN ('queued','running')
                 LIMIT 5",
            )?;
            let rows = stmt
                .query_map([&hash_check], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await?;

    if !dup_names.is_empty() {
        eprintln!("warning: code hash {hash} matches existing entries:");
        for (dup_name, source) in &dup_names {
            eprintln!("  {source}: {dup_name}");
        }
    }

    let hash_insert = hash;
    db.call(move |conn| {
        conn.execute(
            "INSERT INTO backtest_queue \
             (name, code, code_hash, batch, priority, \
              description, hypothesis, based_on, status, \
              project_id)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,'queued',?9)",
            rusqlite::params![
                name,
                code,
                hash_insert,
                batch,
                priority,
                desc,
                hyp,
                based,
                project_id
            ],
        )?;
        Ok(())
    })
    .await?;

    let pid_msg = project_id
        .map(|p| format!(" project={p}"))
        .unwrap_or_default();
    println!("queued {strategy}/{name_log} (priority={priority}{pid_msg})");
    Ok(())
}
