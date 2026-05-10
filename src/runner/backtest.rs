//! Backtest orchestrator: coordinates push → compile → create.
//!
//! Sub-steps are in:
//! - `runner_compile.rs`: push code + compile
//! - `runner_poll.rs`: create backtest + poll until done
//!
//! Polling and result collection happen asynchronously in
//! `pipeline::poll_and_collect` after the slot releases the project lock.

use anyhow::Result;
use tokio::time::Duration;

use super::SlotRunner;
use crate::db::{Db, QueueJob};

/// Bundled parameters for the prep half of the backtest pipeline.
/// (Poll/timeout values for the post-prep pipeline live in `PrepTimeouts` —
/// kept separate so this struct only holds what `prep_backtest` actually uses.)
pub(crate) struct BacktestParams<'a> {
    pub job: &'a QueueJob,
    pub db: &'a Db,
    pub project_id: i64,
    pub pre_compile_id: Option<String>,
    pub compile_poll: Duration,
    pub compile_timeout: Duration,
}

impl SlotRunner {
    /// Prep a backtest: push code (or skip if pre-compiled), compile,
    /// `create_backtest` on QC. Writes `backtest_id` to queue row.
    ///
    /// Holds the project lock throughout; caller must release after this returns.
    /// Does NOT poll or collect — those happen async in `pipeline::poll_and_collect`.
    ///
    /// Returns `(backtest_id, project_id, create_api_secs)`.
    pub(crate) async fn prep_backtest(
        &self,
        params: BacktestParams<'_>,
    ) -> Result<(String, i64, f64)> {
        let BacktestParams {
            job,
            db,
            project_id,
            pre_compile_id,
            compile_poll,
            compile_timeout,
        } = params;
        let slot = self.slot_id;
        let job_name = &job.name;

        tracing::debug!(
            target: "qc::pipeline",
            "slot-{slot}: prep_started project={project_id} job={job_name}"
        );

        // ── Step 1+2: Push code and compile (or skip if pre-compiled) ──
        let (compile_id, compile_phase_dur, push_start, compile_start) = self
            .push_and_compile(
                job,
                project_id,
                pre_compile_id,
                compile_poll,
                compile_timeout,
            )
            .await?;

        let push_secs = compile_start.duration_since(push_start).as_secs_f64();

        tracing::debug!(
            target: "qc::pipeline",
            "slot-{slot}: push_done project={project_id} elapsed={push_secs:.1}s"
        );
        tracing::debug!(
            target: "qc::pipeline",
            "slot-{slot}: compile_done project={project_id} compile_id={compile_id} elapsed={:.1}s",
            compile_phase_dur.as_secs_f64()
        );

        // ── Step 3: Create backtest ──
        let (_bt_resp, backtest_id, bt_create_start) = self
            .create_backtest_on_qc(project_id, &compile_id, job)
            .await?;

        let create_api_secs = bt_create_start.elapsed().as_secs_f64();

        tracing::debug!(
            target: "qc::pipeline",
            "slot-{slot}: create_bt_done project={project_id} backtest_id={backtest_id} elapsed={create_api_secs:.1}s"
        );

        // Persist backtest_id on the queue row while we still hold the lock.
        {
            let bid = backtest_id.clone();
            let jid = job.id;
            let _ = db
                .call(move |conn| {
                    conn.execute(
                        "UPDATE backtest_queue SET backtest_id=?1 \
                         WHERE id=?2 AND status='running'",
                        rusqlite::params![bid, jid],
                    )?;
                    Ok(())
                })
                .await;
        }

        // Record push / compile / create_api counters now. poll_tail and
        // bt_run are recorded in pipeline::poll_and_collect after polling.
        let push_dur = compile_start.duration_since(push_start);
        let create_api_dur = bt_create_start.elapsed();
        let to_ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
        if let Some(s) = &self.session {
            use std::sync::atomic::Ordering;
            // Global atomic counters (used by periodic overhead summary in
            // server_mod::overhead — accurate at long-run scope).
            s.push_ms.fetch_add(to_ms(push_dur), Ordering::SeqCst);
            s.compile_ms
                .fetch_add(to_ms(compile_phase_dur), Ordering::SeqCst);
            s.create_api_ms
                .fetch_add(to_ms(create_api_dur), Ordering::SeqCst);
        }
        // Per-cycle local phase log — replaces the misleading global-atomic-delta
        // version that had cross-slot attribution. These are the genuine per-cycle
        // wall-time durations for THIS slot/job. All three are NODE-IDLE phases:
        // QC node `busy=true` only AFTER create_backtest returns and bt_run starts.
        tracing::debug!(
            target: "qc::pipeline",
            "slot-{slot} {}: phase_breakdown_ms push={} compile={} create_api={} (local per-cycle, all node-idle)",
            job.strategy,
            to_ms(push_dur),
            to_ms(compile_phase_dur),
            to_ms(create_api_dur)
        );

        Ok((backtest_id, project_id, create_api_secs))
    }
}
