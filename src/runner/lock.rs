//! Project locking — ensure exclusive code push per project.

use tokio::time::{Duration, sleep};

use super::SlotRunner;
use crate::db::{Db, QueueJob};

impl SlotRunner {
    /// Lock a project for exclusive code push.
    /// For pre-warmed jobs, the project is already locked.
    /// Returns the locked `project_id`, or None if all projects are locked.
    pub(crate) async fn lock_project(
        &self,
        project_id: i64,
        skip_if_prewarmed: bool,
        strategy: &str,
        job_name: &str,
        job: &QueueJob,
        db: &Db,
    ) -> Option<i64> {
        let lock_start = std::time::Instant::now();
        let record_lock = |session: &Option<std::sync::Arc<crate::server::SessionCounters>>| {
            if let Some(s) = session {
                let ms = u64::try_from(lock_start.elapsed().as_millis()).unwrap_or(u64::MAX);
                s.lock_wait_ms
                    .fetch_add(ms, std::sync::atomic::Ordering::SeqCst);
            }
        };
        if skip_if_prewarmed {
            record_lock(&self.session);
            return Some(project_id);
        }

        let mut locked = self.project_lock.lock().await;

        if !locked.contains(&project_id) {
            locked.insert(project_id);
            tracing::debug!(
                "slot-{}: locked project {} (total locked={})",
                self.slot_id,
                project_id,
                locked.len()
            );
            record_lock(&self.session);
            return Some(project_id);
        }

        // Primary project is locked — find an alternative
        let strat_idx = self.strategies.iter().position(|s| s.name == strategy);

        if let Some(si) = strat_idx {
            for pid in &self.strategies[si].project_ids {
                if !locked.contains(pid) {
                    tracing::debug!(
                        "slot-{}: project {} locked, switching to {} \
                         for {}/{}",
                        self.slot_id,
                        project_id,
                        pid,
                        strategy,
                        job_name
                    );
                    let alt = *pid;
                    let jid = job.id;
                    let db_clone = db.clone();
                    // ATOMIC: insert alt into locked BEFORE dropping the mutex.
                    // Otherwise two concurrent slots can both observe alt as
                    // free, both insert (HashSet dedups silently), both push
                    // to the same QC project. When they unlock, the second
                    // remove() removes a phantom entry — corrupting the lock
                    // state for any concurrent warmer that just locked alt.
                    locked.insert(alt);
                    drop(locked);
                    let _ = db_clone
                        .call(move |conn| {
                            conn.execute(
                                "UPDATE backtest_queue \
                                 SET project_id=?1 WHERE id=?2",
                                rusqlite::params![alt, jid],
                            )?;
                            Ok(())
                        })
                        .await;
                    record_lock(&self.session);
                    return Some(alt);
                }
            }
        }

        // All projects locked — diagnostic: how many do we have, how many in pool?
        let pool_len = if let Some(ref p) = self.warm_pool {
            p.len().await
        } else {
            0
        };
        tracing::warn!(
            "slot-{}: IDLE — all projects locked (pool_len={}), releasing {}/{}; \
             likely warmer over-reserved or slot_reserve too small",
            self.slot_id,
            pool_len,
            strategy,
            job_name
        );
        let jid = job.id;
        let db_clone = db.clone();
        drop(locked);
        let _ = db_clone
            .call(move |conn| {
                conn.execute(
                    "UPDATE backtest_queue SET status='queued', \
                     started_at=NULL, node_id=NULL \
                     WHERE id=?1 AND status='running'",
                    [jid],
                )?;
                Ok(())
            })
            .await;
        sleep(Duration::from_millis(3500)).await;
        None
    }
}
