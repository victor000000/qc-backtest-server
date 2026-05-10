//! Periodic status reporting for the running server.
//!
//! Sub-modules:
//! - `status_query.rs`: `gather_status`, `compute_overhead`
//! - `server_recover.rs`: `recover_running_jobs` (stale job recovery)
//! - `status_emit.rs`: starvation log + overhead delta emission

use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::client::QcClient;
use crate::db::Db;

use super::status_emit::{emit_overhead_delta, log_starvation};

// Re-export so existing `use crate::server::status::recover_running_jobs`
// still works
pub(crate) use super::recover::recover_running_jobs;

/// Inputs for a single status tick.
pub(crate) struct StatusTickInput<'a> {
    pub dbs: &'a [Db],
    pub uptime_secs: u64,
    pub rl_delay_ms: u64,
    pub session_completed: u64,
    pub session_failed: u64,
    pub session: Arc<super::SessionCounters>,
    pub num_slots: usize,
    pub client: &'a Arc<QcClient>,
    pub project_id: i64,
}

/// Periodic status summary for the running server.
pub(crate) async fn log_status(input: StatusTickInput<'_>) {
    use super::status_fmt::{StatusReport, format_status_report};
    use super::status_query::{compute_overhead, gather_status};

    let StatusTickInput {
        dbs,
        uptime_secs,
        rl_delay_ms: _rl_delay_ms,
        session_completed,
        session_failed,
        session,
        num_slots,
        client,
        project_id,
    } = input;

    let throughput = if uptime_secs > 0 {
        let completed = f64::from(u32::try_from(session_completed).unwrap_or(u32::MAX));
        let secs = f64::from(u32::try_from(uptime_secs).unwrap_or(u32::MAX));
        completed / (secs / 60.0)
    } else {
        0.0
    };

    let mut total_queued = 0i64;
    let mut total_running = 0i64;
    let mut strategies = Vec::new();
    let mut recent_errors = Vec::new();

    for db in dbs {
        if let Some((strat, errors)) = gather_status(db).await {
            total_queued += strat.queued;
            total_running += strat.running;
            strategies.push(strat);
            recent_errors.extend(errors);
        }
    }

    // busy_slots = REAL QC busy count from read_project_nodes (count of
    // nodes with busy=true). Updated every 30s by dedicated qc-poller.
    // No local fallback inflation — show truth even if stale.
    // First-ever tick (before poller has run) shows 0; box will catch up
    // within 30s. This matches the spirit: status reflects actual QC state.
    let cached_busy = session.qc_busy_slots.load(Ordering::SeqCst);
    let busy_slots: usize = if cached_busy == usize::MAX {
        0
    } else {
        cached_busy
    };
    let _ = client;
    let _ = project_id;
    let _ = total_running; // consumed elsewhere

    let state = if total_queued == 0 && total_running == 0 {
        "IDLE"
    } else {
        "RUNNING"
    };
    let overhead_pct = compute_overhead(dbs, session_completed).await;

    let report = format_status_report(&StatusReport {
        state,
        uptime_secs,
        throughput,
        session_completed,
        session_failed,
        overhead_pct,
        strategies,
        recent_errors,
        busy_slots,
        total_slots: num_slots,
    });

    tracing::info!("{report}");
    let active_collects = session.active_collects.load(Ordering::SeqCst);
    tracing::info!("  pipeline: active_collects={active_collects}");
    // Reset the per-print completion counter so the next batch of #N
    // prefixes start at #1 — operators read the count as "completions
    // since the last status box".
    if let Ok(mut g) = session.minute_counter.lock() {
        g.1 = 0;
    }
    log_starvation(&session, uptime_secs);
    emit_overhead_delta(&session, num_slots);
}
