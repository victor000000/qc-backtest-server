//! Job acquisition: try warm pool first, then fresh pick. Project locking.

use std::time::Instant;

use tokio::time::{Duration, sleep};

use super::SlotRunner;
use crate::db::{Db, QueueJob};

impl SlotRunner {
    /// Try to get a job from the warm pool, or fall back to `pick_next`.
    /// Returns None if no work is available (already backs off internally).
    pub(crate) async fn acquire_job(
        &mut self,
        backoff: &Duration,
    ) -> Option<(QueueJob, Db, i64, bool, Option<String>)> {
        let acquire_start = Instant::now();
        let record_wait = |session: &Option<std::sync::Arc<crate::server::SessionCounters>>| {
            if let Some(s) = session {
                let ms = u64::try_from(acquire_start.elapsed().as_millis()).unwrap_or(u64::MAX);
                s.pool_wait_ms
                    .fetch_add(ms, std::sync::atomic::Ordering::SeqCst);
            }
        };
        if let Some(ref pool) = self.warm_pool {
            // Fast path: try the pool.
            if let Some(we) = pool.take().await {
                // Rewrite node_id from "warm-pool-N" to "slot-X" so the
                // DB reflects actual ownership. Otherwise status-query
                // counts of "slot-%" running rows miss every pool-launched
                // job and under-report busy slots.
                let jid = we.job.id;
                let slot_tag = format!("slot-{}", self.slot_id);
                let _ = we
                    .db
                    .call(move |conn| {
                        conn.execute(
                            "UPDATE backtest_queue SET node_id=?1 \
                             WHERE id=?2 AND status='running'",
                            rusqlite::params![slot_tag, jid],
                        )?;
                        Ok(())
                    })
                    .await;
                let cid = we.compile_id.clone();
                record_wait(&self.session);
                return Some((we.job, we.db, we.project_id, true, Some(cid)));
            }

            // Pool empty. If no queued work anywhere, long backoff.
            if !self.any_queued_work().await {
                tracing::debug!(
                    "slot-{}: IDLE — no queued work in any DB, backing off {}s",
                    self.slot_id,
                    backoff.as_secs()
                );
                sleep(*backoff).await;
                return None;
            }

            // Pool empty but work exists. Warm-pool entries are pre-compiled
            // (~0s overhead) vs direct-claim (~30-60s push+compile+poll).
            // Brief 1s window for warmer to deliver an entry; then fall through
            // so direct-claim can race in parallel with warmer finishing.
            let wait_start = Instant::now();
            for attempt in 0..4 {
                sleep(Duration::from_millis(175)).await;
                if let Some(we) = pool.take().await {
                    let jid = we.job.id;
                    let slot_tag = format!("slot-{}", self.slot_id);
                    let _ = we
                        .db
                        .call(move |conn| {
                            conn.execute(
                                "UPDATE backtest_queue SET node_id=?1 \
                                 WHERE id=?2 AND status='running'",
                                rusqlite::params![slot_tag, jid],
                            )?;
                            Ok(())
                        })
                        .await;
                    tracing::debug!(
                        "slot-{}: warm entry arrived after {}ms (attempt {})",
                        self.slot_id,
                        wait_start.elapsed().as_millis(),
                        attempt + 1
                    );
                    let cid = we.compile_id.clone();
                    record_wait(&self.session);
                    return Some((we.job, we.db, we.project_id, true, Some(cid)));
                }
            }

            // Still nothing after ~1s. Fall through to direct-claim so we
            // don't leave the slot idle, but log at debug (expected during
            // warm-pool cold start).
            tracing::debug!(
                "slot-{}: pool empty after 1s wait (len={}), falling to direct-claim",
                self.slot_id,
                pool.len().await
            );
        }

        // No warm pool or pool was empty — pick fresh
        let picked = self.pick_next().await;
        self.job_counter += 1;

        if let Some((j, d, p)) = picked {
            record_wait(&self.session);
            Some((j, d, p, false, None))
        } else {
            tracing::debug!(
                "slot-{}: IDLE — pick_next returned None, backing off {}s",
                self.slot_id,
                backoff.as_secs()
            );
            sleep(*backoff).await;
            None
        }
    }

    // lock_project is in runner_lock.rs
    // any_queued_work is in queued_check.rs
}
