//! Create backtest on QC.

use std::time::Instant;

use anyhow::{Result, bail};

use super::SlotRunner;
use crate::db::QueueJob;
use crate::rate_limit::retry_on_rate_limit;

impl SlotRunner {
    /// Create a backtest on QC. Fails fast on "no spare nodes".
    ///
    /// Returns `(response, backtest_id, create_start_time)`.
    pub(crate) async fn create_backtest_on_qc(
        &self,
        project_id: i64,
        compile_id: &str,
        job: &QueueJob,
    ) -> Result<(crate::models::backtest::BacktestResponse, String, Instant)> {
        let slot = self.slot_id;
        let job_name = &job.name;

        tracing::debug!(
            "slot-{slot}: [{job_name}] creating backtest \
             on project {project_id}"
        );
        let bt_create_start = Instant::now();

        // 2026-05-07: semaphore acquisition moved INSIDE the retry closure.
        // Pre-fix, the permit was held across retry_on_rate_limit's full
        // 3-attempt × 8s exponential-backoff window — so during a rate-limit
        // storm, 6 stuck slots could hold permits for 24s while OTHER slots
        // queued up to 33s in qc_create_sem_wait (observed restart15:
        // sem_wait_ms=33062). Releasing the permit between retries lets the
        // queue rotate during storm cooldown — no one slot blocks the fleet.
        // 2026-05-07: dropped max_retries 1→0. Restart27 cycle p99=28s,
        // all from 1-retry chains (~25s create_api). With max_retries=0 the
        // slot bails immediately on rate-limit → ~0.4s prep_failed → rotates
        // to a different project → next prep ~1s + pacer wait. Total
        // recovery ~7s vs ~28s, freeing ~21s of slot time per rate-limit
        // hit (177 hits/9min in restart25 = ~7 min slot time recovered).
        // Push+compile are sub-second so re-prep cost is negligible.
        let bt = retry_on_rate_limit(self.rl_state.as_ref(), 0, || {
            let cid = compile_id.to_string();
            let name = job.name.clone();
            async move {
                let sem_wait_start = Instant::now();
                let _permit = match self.create_semaphore.as_ref() {
                    Some(sem) => Some(sem.clone().acquire_owned().await?),
                    None => None,
                };
                let sem_wait_ms = sem_wait_start.elapsed().as_millis();
                if sem_wait_ms > 100 {
                    tracing::debug!(
                        target: "qc::api",
                        "qc_create_sem_wait slot-{slot} job={job_name} sem_wait_ms={sem_wait_ms}"
                    );
                }

                if let Some(rls) = self.rl_state.as_ref() {
                    let pacer_wait = rls.claim_create_slot();
                    if pacer_wait.as_millis() > 50 {
                        tracing::debug!(
                            target: "qc::api",
                            "qc_create_pacer_wait slot-{slot} job={job_name} wait_ms={}",
                            pacer_wait.as_millis()
                        );
                        tokio::time::sleep(pacer_wait).await;
                    }
                }
                let resp = self
                    .client
                    .create_backtest(project_id, &cid, &name, None)
                    .await?;

                if !resp.success {
                    let err_msg = format!("{:?}", resp.errors);
                    let lower = err_msg.to_lowercase();

                    if lower.contains("too many") || lower.contains("slow down") {
                        // Push the global pacer forward so every slot waits
                        // through the cooldown — kills the retry-storm pattern
                        // where N slots fail simultaneously, then all retry
                        // simultaneously, and re-fail simultaneously.
                        if let Some(rls) = self.rl_state.as_ref() {
                            rls.record_rate_limit_hit();
                        }
                        tracing::debug!(
                            target: "qc::api",
                            "qc_rate_limit_hit slot-{slot} job={job_name} project={project_id} (pacer +5s, will retry with backoff)"
                        );
                        bail!("rate_limit: {err_msg}");
                    }
                    if lower.contains("no spare nodes") {
                        bail!("no spare nodes: {err_msg}");
                    }
                    bail!("backtest_create: {err_msg}");
                }
                Ok(resp)
            }
        })
        .await?;

        let backtest_id = bt
            .backtest
            .as_ref()
            .map(|b| b.backtest_id.clone())
            .unwrap_or_default();

        if backtest_id.is_empty() {
            bail!("backtest_create: no backtestId returned");
        }

        tracing::debug!(
            "slot-{slot}: [{job_name}] backtest created: \
             {backtest_id} in {:.1}s",
            bt_create_start.elapsed().as_secs_f64()
        );

        Ok((bt, backtest_id, bt_create_start))
    }
}
