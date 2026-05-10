//! Result collection: read backtest stats from QC and store in experiments.
//!
//! Sub-modules:
//! - `runner_collect_parse.rs`: `parse_and_store` (stat extraction + DB insert)

use anyhow::Result;

use super::collect_parse::parse_and_store;
use crate::client::QcClient;
use crate::db::{Db, QueueJob};
use crate::models::backtest::BacktestResponse;
use crate::rate_limit::stat_i64;

/// Collect results directly from a poll response (no extra API call).
/// If the poll response has empty stats (QC populates async), falls back
/// to retry-read.
///
/// # Errors
/// Returns `Err` on HTTP transport failure during retry-read or SQL error
/// when marking the job failed or inserting experiment rows.
pub async fn collect_from_response(
    client: &QcClient,
    job: &QueueJob,
    db: &Db,
    project_id: i64,
    backtest_id: &str,
    runtime_secs: f64,
    response: BacktestResponse,
) -> Result<Option<f64>> {
    let has_stats = response.backtest.as_ref().is_some_and(|bt| {
        !bt.statistics.is_empty() && stat_i64(&bt.statistics, "Total Orders").unwrap_or(0) > 0
    });

    if has_stats {
        tracing::debug!(
            target: "qc::pipeline",
            "qc_collect_path job={} stats_count={} path=fast",
            job.name,
            response.backtest.as_ref().map_or(0, |bt| bt.statistics.len())
        );
        return parse_and_store(job, db, project_id, backtest_id, runtime_secs, &response).await;
    }

    // Slow path: QC set completed=true before populating stats.
    let stats_count = response
        .backtest
        .as_ref()
        .map_or(0, |bt| bt.statistics.len());
    tracing::debug!(
        target: "qc::pipeline",
        "qc_collect_path job={} stats_count={stats_count} path=slow_retry",
        job.name
    );
    // QC async-populates statistics after marking completed=true.
    // Large backtests (9k+ orders on 311 ETFs) can take 60s+ to post-process.
    // 2026-05-08 evidence (1651 collects): 93% take slow-retry path; of those,
    // 77% finish at retry-1 (1s), 23% at retry-2 (3s), 0.4% past retry-2.
    // Old [1,2,3,5,8,10,15,20,25,30,30,30] over-spent on the 0.4% tail.
    //
    // Tunings:
    // 1. retry-2 delay 2s→1s. The 23% retry-2 cohort moves collect=4s→3s
    //    (saves ~1s on ~480 BTs/day). Tiny risk that 2s isn't enough QC
    //    populate-time and a small slice slips to retry-3 (now total 4s).
    // 2. First-delay jitter 0-500ms. Of 78 /backtests/read 502s today, 41%
    //    fired in the same second as another (real convoy). Jitter spreads
    //    the retry-1 fan-out across 500ms. Mean delay is unchanged (collect
    //    doesn't block slot occupancy — see collect_phase.rs release_after_poll).
    // 3. Tighten downstream [3,5,8,10]→[2,3,5,8] — same total budget, faster
    //    ramp for the rare retry-3+ path. Total back-off ~170s (was 179s).
    // Delays in milliseconds (70% of original 1/1/2/3/5/8/15/20/25/30/30/30s budget).
    let retry_delays = [
        700u64, 700, 1400, 2100, 3500, 5600, 10500, 14000, 17500, 21000, 21000, 21000,
    ];
    for (attempt, delay) in retry_delays.iter().enumerate() {
        let attempt = attempt + 1;
        let jitter_ms = if attempt == 1 {
            fastrand::u64(0..350)
        } else {
            0
        };
        tokio::time::sleep(std::time::Duration::from_millis(delay + jitter_ms)).await;
        match client.read_backtest(project_id, backtest_id).await {
            Ok(r) => {
                let ok = r.backtest.as_ref().is_some_and(|bt| {
                    !bt.statistics.is_empty()
                        && stat_i64(&bt.statistics, "Total Orders").unwrap_or(0) > 0
                });
                if ok {
                    tracing::debug!("collect: {} stats populated on retry {attempt}", job.name);
                    return parse_and_store(job, db, project_id, backtest_id, runtime_secs, &r)
                        .await;
                }
                tracing::debug!(
                    "collect: {} retry {attempt}/{} still empty",
                    job.name,
                    retry_delays.len()
                );
            }
            Err(e) => {
                tracing::warn!(
                    "collect: {} retry {attempt}/{} read error: {e}",
                    job.name,
                    retry_delays.len()
                );
            }
        }
    }

    let err_msg = format!(
        "stats_not_returned: QC returned completed=true but statistics \
         never populated after {} retries",
        retry_delays.len()
    );
    tracing::error!("collect: {} {err_msg}", job.name);
    // Cancel the QC backtest so its node can be reused. The backtest itself
    // already finished from QC's perspective — calling delete just removes
    // the orphaned record so it doesn't keep occupying the project's
    // backtest list.
    crate::runner::pipeline::cancel_qc_backtest(
        client,
        project_id,
        backtest_id,
        0,
        &job.name,
        "stats_retry",
    )
    .await;
    db.mark_failed(job.id, &err_msg, None).await?;
    Ok(None)
}
