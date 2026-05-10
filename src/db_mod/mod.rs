//! QC-specific database wrapper.
//!
//! Submodules:
//! - `claim`: job claiming (`claim_next_job`, `claim_next_job_with_pool`)
//! - `types`: data structs (`QueueJob`, Experiment)

pub mod claim;
mod claim_scan;
pub mod experiment;
pub mod types;

use std::ops::Deref;
use std::path::Path;

use anyhow::Result;

use crate::database::Db as BaseDb;

// Re-export types so `crate::db::QueueJob` still works everywhere
pub use types::{Experiment, QueueJob};

/// QC-specific database wrapper.
/// Derefs to `shared::Db` for transparent access to settings, `call()`, etc.
#[derive(Clone)]
pub struct Db {
    inner: BaseDb,
    pub strategy: String,
}

impl Deref for Db {
    type Target = BaseDb;
    fn deref(&self) -> &BaseDb {
        &self.inner
    }
}

impl Db {
    /// Open a strategy-scoped database at `path`.
    ///
    /// # Errors
    /// Returns `Err` if the underlying `SQLite` connection cannot be opened.
    pub fn open(path: &Path, strategy: &str) -> Result<Self> {
        let db = BaseDb::open(path)?;
        Ok(Self {
            inner: db,
            strategy: strategy.to_string(),
        })
    }

    // ── Queue status ────────────────────────────────────────────

    /// Mark a job as done with `compile_id`, `backtest_id`, and runtime.
    ///
    /// # Errors
    /// Returns `Err` on SQL execution error.
    pub async fn mark_done(
        &self,
        job_id: i64,
        compile_id: &str,
        backtest_id: &str,
        runtime_secs: f64,
        result_json: Option<&str>,
    ) -> Result<()> {
        tracing::debug!(
            "{}: mark_done job_id={job_id} bt={backtest_id} \
             runtime={runtime_secs:.1}s",
            self.strategy
        );
        let cid = compile_id.to_string();
        let bid = backtest_id.to_string();
        let rj = result_json.map(std::string::ToString::to_string);
        self.inner
            .call(move |conn| {
                conn.execute(
                    "UPDATE backtest_queue
                     SET status='done', completed_at=datetime('now'),
                         compile_id=?1, backtest_id=?2,
                         runtime_seconds=?3, result_json=?4
                     WHERE id=?5",
                    rusqlite::params![cid, bid, runtime_secs, rj, job_id],
                )?;
                Ok(())
            })
            .await
    }

    /// Mark a job as failed with error message.
    ///
    /// # Errors
    /// Returns `Err` on SQL execution error.
    pub async fn mark_failed(
        &self,
        job_id: i64,
        error: &str,
        result_json: Option<&str>,
    ) -> Result<()> {
        tracing::debug!(
            "{}: mark_failed job_id={job_id} error={}",
            self.strategy,
            &error[..error.len().min(80)]
        );
        let err = error.to_string();
        let rj = result_json.map(std::string::ToString::to_string);
        self.inner
            .call(move |conn| {
                conn.execute(
                    "UPDATE backtest_queue
                     SET status='failed', completed_at=datetime('now'),
                         error_message=?1, retry_count=retry_count+1,
                         result_json=?2
                     WHERE id=?3",
                    rusqlite::params![err, rj, job_id],
                )?;
                Ok(())
            })
            .await
    }

    // insert_experiment is in experiment.rs

    // ── Status ───────────────────────────────────────────────────

    /// Return `(status, count)` pairs for the `backtest_queue` table.
    ///
    /// # Errors
    /// Returns `Err` on SQL execution error.
    pub async fn queue_counts(&self) -> Result<Vec<(String, i64)>> {
        self.inner
            .call(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT status, COUNT(*) FROM backtest_queue \
                     GROUP BY status",
                )?;
                let rows = stmt
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .await
    }
}
