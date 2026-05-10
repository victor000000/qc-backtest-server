//! Job claiming: atomically pick + mark running in one transaction.

use anyhow::Result;

use super::Db;
use super::claim_scan::scan_candidates;
use super::types::QueueJob;

impl Db {
    /// Atomically claim the next queued job.
    ///
    /// # Errors
    /// Returns `Err` on SQL execution error.
    pub async fn claim_next_job(
        &self,
        node_id: &str,
        project_id: i64,
        name_prefix: Option<&str>,
    ) -> Result<Option<(QueueJob, i64)>> {
        self.claim_next_job_with_pool(node_id, project_id, name_prefix, &[])
            .await
    }

    /// Like `claim_next_job` but with fallback `project_ids`.
    ///
    /// # Errors
    /// Returns `Err` on SQL execution error.
    pub async fn claim_next_job_with_pool(
        &self,
        node_id: &str,
        project_id: i64,
        name_prefix: Option<&str>,
        project_pool: &[i64],
    ) -> Result<Option<(QueueJob, i64)>> {
        let strategy = self.strategy.clone();
        let nid = node_id.to_string();
        let prefix = name_prefix.map(std::string::ToString::to_string);
        let pool = project_pool.to_vec();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction()?;

            // Collect running project_ids
            let mut running_projects: Vec<i64> = Vec::new();
            {
                let mut stmt = tx.prepare(
                    "SELECT DISTINCT project_id FROM backtest_queue \
                     WHERE status='running' AND project_id IS NOT NULL",
                )?;
                let rows = stmt.query_map([], |row| row.get::<_, i64>(0))?;
                for p in rows.flatten() {
                    running_projects.push(p);
                }
            }

            // Find candidates
            let candidates: Vec<(i64, Option<i64>)> = {
                let query = if let Some(p) = &prefix {
                    format!(
                        "SELECT id, project_id FROM backtest_queue
                         WHERE status='queued' AND name LIKE '{p}%'
                         ORDER BY priority ASC, id ASC
                         LIMIT 20"
                    )
                } else {
                    "SELECT id, project_id FROM backtest_queue
                     WHERE status='queued'
                     ORDER BY priority ASC, id ASC
                     LIMIT 20"
                        .to_string()
                };
                let mut stmt = tx.prepare(&query)?;
                stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                    .filter_map(std::result::Result::ok)
                    .collect()
            };

            let result = scan_candidates(&candidates, &running_projects, project_id, &pool);

            tracing::debug!(
                "{nid}: claim scan: {} candidates, {} skipped \
                 (project conflicts), running_projects={running_projects:?}",
                candidates.len(),
                result.skipped
            );

            let Some(job_id) = result.job_id else {
                tracing::debug!(
                    "{nid}: no claimable job \
                     (all {} candidates conflict)",
                    result.skipped
                );
                return Ok(None);
            };

            let effective_project_id = result.effective_project_id;

            tx.execute(
                "UPDATE backtest_queue
                 SET status='running', started_at=datetime('now'),
                     node_id=?1, project_id=?2
                 WHERE id=?3 AND status='queued'",
                rusqlite::params![nid, effective_project_id, job_id],
            )?;

            let job = tx.query_row(
                "SELECT id, name, code, description, hypothesis,
                        based_on, batch, priority
                 FROM backtest_queue WHERE id=?1",
                [job_id],
                |row| {
                    Ok(QueueJob {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        code: row.get(2)?,
                        description: row.get(3)?,
                        hypothesis: row.get(4)?,
                        based_on: row.get(5)?,
                        batch: row.get(6)?,
                        priority: row.get(7)?,
                        strategy: strategy.clone(),
                    })
                },
            )?;

            tx.commit()?;
            Ok(Some((job, effective_project_id)))
        })
        .await
    }
}
