//! Poll a running backtest until completion.

mod tier;

use std::time::Instant;

use anyhow::{Result, bail};
use tokio::time::{Duration, sleep};

use crate::rate_limit::is_rate_limit;

use self::tier::tiered_poll_delay;

/// Free-function variant of `poll_backtest` for use in spawned pipeline tasks
/// (no `&self` needed; takes the `client` directly). Uses adaptive tiered
/// delays + per-task 429 elasticity instead of a fixed interval.
pub(crate) async fn poll_backtest_unlocked(
    client: &crate::client::QcClient,
    project_id: i64,
    backtest_id: &str,
    job_name: &str,
    _bt_poll_hint: Duration, // ignored — tiers chosen internally
    bt_timeout: Duration,
) -> Result<(crate::models::backtest::BacktestResponse, f64, u32)> {
    tracing::debug!("[{job_name}] polling backtest {backtest_id} (tiered)");
    let bt_run_start = Instant::now();
    let deadline = Instant::now() + bt_timeout;
    let mut poll_count: u32 = 0;
    let mut progress: f64 = 0.0;
    let mut backoff_factor: u32 = 1; // doubles on 429; resets on success

    loop {
        let delay = tiered_poll_delay(poll_count + 1, progress, backoff_factor);
        if !delay.is_zero() {
            sleep(delay).await;
        }
        poll_count += 1;

        tracing::debug!(
            target: "qc::pipeline",
            "[{job_name}] poll_attempt #{poll_count} delay_before={:.3}s progress={progress:.2} backoff={backoff_factor}x",
            delay.as_secs_f64()
        );

        // Time the read so we can attribute slow tails to a specific
        // backtest/poll. The lower-level qc_api_call log knows latency but
        // not which backtest the poll belongs to — without this it's hard to
        // tell whether QC stalls on one backtest or fleet-wide.
        let read_start = Instant::now();
        let r = match client.read_backtest(project_id, backtest_id).await {
            Ok(r) => {
                let read_ms = read_start.elapsed().as_millis();
                if read_ms > 3_000 {
                    tracing::debug!(
                        target: "qc::pipeline",
                        "qc_poll_slow job={job_name} bt={backtest_id} poll={poll_count} read_ms={read_ms} progress={progress:.2}"
                    );
                }
                if backoff_factor > 1 {
                    tracing::debug!(
                        target: "qc::pipeline",
                        "[{job_name}] backoff_reset poll={poll_count} (was {backoff_factor}x)"
                    );
                }
                backoff_factor = 1; // reset on success
                r
            }
            Err(e) if is_rate_limit(&e) => {
                // Per-task 429 elasticity: double for next polls in THIS task
                backoff_factor = (backoff_factor * 2).min(4);
                tracing::debug!(
                    target: "qc::pipeline",
                    "[{job_name}] rate_limit on poll {poll_count} — backoff_factor={backoff_factor}"
                );
                continue;
            }
            Err(e) => {
                tracing::debug!(
                    target: "qc::pipeline",
                    "[{job_name}] poll_error poll={poll_count} error={e}"
                );
                return Err(e);
            }
        };

        if let Some(bt_result) = r.backtest.as_ref() {
            let new_progress = bt_result.progress;
            if (new_progress - progress).abs() > 0.01 {
                tracing::debug!(
                    target: "qc::pipeline",
                    "[{job_name}] progress_update poll={poll_count} progress={new_progress:.2} status={:?}",
                    bt_result.status.as_deref().unwrap_or("?")
                );
            }
            progress = new_progress;
            if bt_result.completed {
                if let Some(err) = &bt_result.error
                    && !err.is_empty()
                {
                    tracing::debug!(
                        target: "qc::pipeline",
                        "[{job_name}] runtime_error_detected poll={poll_count} error={err}"
                    );
                    bail!("runtime_error: {err}");
                }
                let bt_phase_secs = bt_run_start.elapsed().as_secs_f64();
                tracing::debug!(
                    target: "qc::pipeline",
                    "[{job_name}] tier_summary polls={poll_count} bt_run={bt_phase_secs:.1}s final_progress={progress:.2}"
                );
                return Ok((r, bt_phase_secs, poll_count));
            }
        }

        if Instant::now() > deadline {
            bail!(
                "backtest_timeout after {}s ({poll_count} polls)",
                bt_timeout.as_secs()
            );
        }
    }
}
