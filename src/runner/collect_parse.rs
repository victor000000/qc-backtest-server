//! Parse backtest stats from QC response and store in experiments table.

#![allow(
    clippy::too_many_lines,
    reason = "parses ~27 QC stat fields into Experiment struct; flat mapping is clearest"
)]

use std::time::Instant;

use anyhow::Result;

use crate::db::{Db, Experiment, QueueJob};
use crate::models::backtest::BacktestResponse;
use crate::rate_limit::{stat_f64, stat_i64};

/// Parse stats from a `BacktestResponse` and store in experiments table.
pub(crate) async fn parse_and_store(
    job: &QueueJob,
    db: &Db,
    project_id: i64,
    backtest_id: &str,
    runtime_secs: f64,
    r: &BacktestResponse,
) -> Result<Option<f64>> {
    let result_json = String::new();

    let bt = r
        .backtest
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no backtest in response"))?;

    let stats = &bt.statistics;
    let total_orders = stat_i64(stats, "Total Orders");
    tracing::debug!(
        "collect: {} orders={:?} stats_count={}",
        job.name,
        total_orders,
        stats.len()
    );

    let result_status = if total_orders == Some(0) || total_orders.is_none() {
        "zero_orders"
    } else if stats.is_empty() {
        "no_stats"
    } else {
        "success"
    };

    let cagr = stat_f64(stats, "Compounding Annual Return");
    let dd = stat_f64(stats, "Drawdown");
    let car_mdd = match (cagr, dd) {
        (Some(c), Some(d)) if d.abs() > 0.001 => Some(c / d),
        _ => None,
    };

    let code_hash = {
        use sha2::{Digest, Sha256};
        let h = Sha256::digest(job.code.as_bytes());
        Some(hex::encode(&h[..8]))
    };

    let exp = Experiment {
        name: job.name.clone(),
        backtest_id: backtest_id.to_string(),
        batch: job.batch.clone(),
        status: result_status.to_string(),
        description: job.description.clone(),
        hypothesis: job.hypothesis.clone(),
        based_on: job.based_on.clone(),
        start_date: bt.backtest_start.clone(),
        end_date: bt.backtest_end.clone(),
        cagr,
        dd,
        car_mdd,
        sharpe: stat_f64(stats, "Sharpe Ratio"),
        sortino: stat_f64(stats, "Sortino Ratio"),
        total_orders,
        win_rate: stat_f64(stats, "Win Rate"),
        alpha: stat_f64(stats, "Alpha"),
        beta: stat_f64(stats, "Beta"),
        expectancy: stat_f64(stats, "Expectancy"),
        profit_loss_ratio: stat_f64(stats, "Profit-Loss Ratio"),
        annual_std_dev: stat_f64(stats, "Annual Standard Deviation"),
        annual_variance: stat_f64(stats, "Annual Variance"),
        information_ratio: stat_f64(stats, "Information Ratio"),
        tracking_error: stat_f64(stats, "Tracking Error"),
        treynor_ratio: stat_f64(stats, "Treynor Ratio"),
        probabilistic_sharpe: stat_f64(stats, "Probabilistic Sharpe Ratio"),
        net_profit_pct: stat_f64(stats, "Net Profit"),
        loss_rate: stat_f64(stats, "Loss Rate"),
        total_fees: stat_f64(stats, "Total Fees"),
        start_equity: stat_f64(stats, "Start Equity"),
        end_equity: stat_f64(stats, "End Equity"),
        portfolio_turnover: stat_f64(stats, "Portfolio Turnover"),
        drawdown_recovery: stat_i64(stats, "Drawdown Recovery"),
        estimated_capacity: stat_f64(stats, "Estimated Strategy Capacity"),
        lowest_capacity_asset: stats.get("Lowest Capacity Asset").cloned(),
        avg_win: stat_f64(stats, "Average Win"),
        avg_loss: stat_f64(stats, "Average Loss"),
        backtest_run_start: bt.created.clone(),
        backtest_run_end: None,
        code_hash,
        runtime_seconds: Some(runtime_secs),
        project_id: Some(project_id),
    };

    // 2026-05-07: instrument the two DB writes — restart30 saw collect_avg=3s
    // tight band, but restart33 with 500ms threshold logged 0 events (DB
    // individually fast). Lowered threshold to 100ms so we can see what's
    // happening, and ALSO log the gap between mark_done finishing and the
    // task_total clock — that's where the 3s actually lives.
    let t_collect_start = Instant::now();
    let t_insert = Instant::now();
    db.insert_experiment(exp).await?;
    let insert_ms = t_insert.elapsed().as_millis();

    let t_mark = Instant::now();
    db.mark_done(job.id, "", backtest_id, runtime_secs, Some(&result_json))
        .await?;
    let mark_ms = t_mark.elapsed().as_millis();

    let collect_ms = t_collect_start.elapsed().as_millis();
    if collect_ms > 100 {
        tracing::debug!(
            target: "qc::pipeline",
            "qc_collect_db job={} insert_ms={insert_ms} mark_done_ms={mark_ms} total_collect_parse_ms={collect_ms}",
            job.name
        );
    }

    tracing::debug!(
        job = %job.name,
        status = result_status,
        car_mdd = ?car_mdd,
        cagr = ?cagr,
        dd = ?dd,
        orders = ?total_orders,
        project = project_id,
        backtest = backtest_id,
        runtime_s = format!("{runtime_secs:.1}"),
        stats_count = stats.len(),
        "experiment stored"
    );

    Ok(car_mdd)
}
