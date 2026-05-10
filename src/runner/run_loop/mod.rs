//! Main loop orchestrator for `SlotRunner`.
//! Submodules: backpressure, cycle counters, prep dispatch.

mod backpressure;
mod cycle;
mod prep;

use std::time::Instant;

use tokio::time::Duration;

use super::SlotRunner;

impl SlotRunner {
    /// Main loop: pick job → lock project → run backtest → collect → unlock.
    pub async fn run(mut self) {
        let backoff = Duration::from_millis(700);
        let timeouts = prep::PrepTimeouts {
            poll_interval: Duration::from_millis(1050),
            backtest_timeout: Duration::from_mins(15),
        };
        let compile_poll = Duration::from_millis(700);
        let compile_timeout = Duration::from_mins(1);

        // Stagger slot startup to avoid thundering herd on API
        tokio::time::sleep(Duration::from_millis(
            u64::try_from(self.slot_id).unwrap_or(0) * 210,
        ))
        .await;
        tracing::debug!("slot-{}: started", self.slot_id);

        // Instant at which the previous cycle *ended* (i.e. became ready to
        // start the next one). First cycle has no previous end, so idle_ms
        // is not recorded.
        let mut prev_cycle_end: Option<Instant> = None;

        loop {
            let cycle_start = Instant::now();
            backpressure::apply(self.session.as_ref(), self.slot_id, self.num_slots).await;

            // Record idle time: gap between previous cycle end and this start.
            if let Some(end) = prev_cycle_end {
                let idle_ms =
                    u64::try_from(cycle_start.duration_since(end).as_millis()).unwrap_or(u64::MAX);
                if let Some(s) = &self.session {
                    s.idle_ms
                        .fetch_add(idle_ms, std::sync::atomic::Ordering::SeqCst);
                }
            }

            // Counters snapshot at cycle start so we can compute per-cycle
            // overhead (excluding bt_run) for the per-strategy record.
            let phase_snapshot_at_start = cycle::snapshot_phases(self.session.as_ref());

            // ── PHASE 1: Acquire a job ──
            let Some((job, db, project_id, skip_push, pre_compile_id)) =
                self.acquire_job(&backoff).await
            else {
                // Idle path — no cycle completed; do not update
                // prev_cycle_end so next iteration's idle_ms captures
                // the full back-to-back gap.
                continue;
            };

            let pick_ms = cycle_start.elapsed().as_millis();
            let job_name = job.name.clone();
            let strategy = job.strategy.clone();

            // ── PHASE 2: Lock project ──
            let Some(project_id) = self
                .lock_project(project_id, skip_push, &strategy, &job_name, &job, &db)
                .await
            else {
                continue;
            };

            tracing::debug!(
                "slot-{}: picked {strategy}/{job_name} project={project_id} pre_compiled={skip_push}",
                self.slot_id,
            );

            // ── PHASE 3: Prep backtest (push+compile+create) ──
            let prep_start = Instant::now();
            tracing::debug!(
                target: "qc::pipeline",
                "slot-{}: lock_acquired project={project_id} strategy={strategy} job={job_name}",
                self.slot_id
            );
            let prep_result = self
                .prep_backtest(super::backtest::BacktestParams {
                    job: &job,
                    db: &db,
                    project_id,
                    pre_compile_id,
                    compile_poll,
                    compile_timeout,
                })
                .await;
            let prep_elapsed = prep_start.elapsed();
            tracing::debug!(
                target: "qc::pipeline",
                "slot-{}: prep_returned project={project_id} elapsed={:.2}s ok={}",
                self.slot_id, prep_elapsed.as_secs_f64(), prep_result.is_ok()
            );

            // ── PHASE 4: Unlock project EARLY (before poll+collect) ──
            //    poll_and_collect only needs (project_id, backtest_id);
            //    QC holds the compiled binary snapshot at create time.
            cycle::release_project_lock(&self.project_lock, self.slot_id, project_id).await;

            prep::dispatch_prep_result(
                &mut self,
                prep::DispatchArgs {
                    prep_result,
                    db: &db,
                    job: &job,
                    project_id,
                    strategy: &strategy,
                    job_name: &job_name,
                    prep_elapsed,
                    prep_start,
                    timeouts: &timeouts,
                },
            )
            .await;

            cycle::record_strategy_cycle(self.session.as_ref(), phase_snapshot_at_start, &strategy);
            cycle::log_cycle_done(
                self.slot_id,
                cycle_start.elapsed().as_millis(),
                pick_ms,
                prep_elapsed.as_millis(),
                &strategy,
                &job_name,
            );

            prev_cycle_end = Some(Instant::now());
        }
    }
}
