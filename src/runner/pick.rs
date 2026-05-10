//! Job picking logic — weighted random selection across strategy queues.

use super::SlotRunner;
use crate::db::{Db, QueueJob};

impl SlotRunner {
    /// Fair weighted pick across all strategy queues.
    ///
    /// 1. Find which strategies have queued work.
    /// 2. Weighted random selection (weight from `strategy/scheduler_weight`).
    /// 3. Atomically claim from the chosen strategy.
    /// 4. If claim fails (race), try other strategies.
    pub(crate) async fn pick_next(&mut self) -> Option<(QueueJob, Db, i64)> {
        let slot = self.slot_id;

        // 1. Peek which strategies have queued work
        let mut available: Vec<(usize, f64)> = Vec::new();

        for (i, db) in self.dbs.iter().enumerate() {
            let prefix = self.name_prefix.clone();
            let has_work = db
                .call(move |conn| {
                    let n: i64 = match &prefix {
                        Some(p) => conn.query_row(
                            "SELECT COUNT(*) FROM backtest_queue \
                             WHERE status='queued' AND name LIKE ?1 LIMIT 1",
                            [format!("{p}%")],
                            |r| r.get(0),
                        )?,
                        None => conn.query_row(
                            "SELECT COUNT(*) FROM backtest_queue \
                             WHERE status='queued' LIMIT 1",
                            [],
                            |r| r.get(0),
                        )?,
                    };
                    Ok(n > 0)
                })
                .await
                .unwrap_or(false);

            tracing::debug!(
                "slot-{slot}: pick_next: strategy[{i}]={} has_work={has_work}",
                self.strategies[i].name
            );

            if has_work {
                available.push((i, self.strategies[i].weight));
            }
        }

        if available.is_empty() {
            tracing::debug!("slot-{slot}: pick_next: no strategies have work");
            return None;
        }

        // 2. Weighted random selection
        let total_weight: f64 = available.iter().map(|(_, w)| w).sum();
        let mut roll = fastrand::f64() * total_weight;
        let mut selected_idx = available[0].0;

        for &(idx, weight) in &available {
            roll -= weight;
            if roll <= 0.0 {
                selected_idx = idx;
                break;
            }
        }

        tracing::debug!(
            "slot-{slot}: pick_next: selected strategy[{selected_idx}]={}",
            self.strategies[selected_idx].name
        );

        // 3. Claim from the selected strategy
        let result = self.try_claim(selected_idx).await;
        if result.is_some() {
            return result;
        }

        // 4. Selected strategy failed — try others
        for &(idx, _) in &available {
            if idx == selected_idx {
                continue;
            }
            let result = self.try_claim(idx).await;
            if result.is_some() {
                return result;
            }
        }

        tracing::debug!("slot-{slot}: pick_next: all claims failed");
        None
    }

    /// Try to claim a job from a specific strategy index.
    async fn try_claim(&self, strat_idx: usize) -> Option<(QueueJob, Db, i64)> {
        let db = &self.dbs[strat_idx];
        let strat = &self.strategies[strat_idx];
        let project_id =
            strat.project_ids[(self.slot_id + self.job_counter) % strat.project_ids.len()];
        let node_tag = format!("slot-{}", self.slot_id);

        let claim_result = db
            .claim_next_job_with_pool(
                &node_tag,
                project_id,
                self.name_prefix.as_deref(),
                &strat.project_ids,
            )
            .await;

        match claim_result {
            Ok(Some((job, effective_project_id))) => {
                tracing::debug!(
                    "slot-{}: claimed {} from {} (project={effective_project_id})",
                    self.slot_id,
                    job.name,
                    strat.name
                );
                Some((job, db.clone(), effective_project_id))
            }
            Ok(None) => {
                tracing::debug!(
                    "slot-{}: claim returned None for {} (project conflicts)",
                    self.slot_id,
                    strat.name
                );
                None
            }
            Err(e) => {
                tracing::warn!("slot-{}: claim error for {}: {e}", self.slot_id, strat.name);
                None
            }
        }
    }
}
