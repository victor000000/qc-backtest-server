//! Experiment insertion: store completed backtest results.

use anyhow::Result;

use super::Db;
use super::types::Experiment;

impl Db {
    /// Insert a completed backtest into the experiments table.
    ///
    /// # Errors
    /// Returns `Err` on SQL execution error.
    pub async fn insert_experiment(&self, exp: Experiment) -> Result<()> {
        self.inner
            .call(move |conn| {
                conn.execute(
                    "INSERT OR REPLACE INTO experiments (
                        name, backtest_id, batch, status, description,
                        hypothesis, based_on, start_date, end_date,
                        cagr, dd, car_mdd, sharpe, sortino,
                        total_orders, win_rate, alpha, beta,
                        expectancy, profit_loss_ratio,
                        annual_std_dev, annual_variance,
                        information_ratio, tracking_error,
                        treynor_ratio, probabilistic_sharpe,
                        net_profit_pct, loss_rate, total_fees,
                        start_equity, end_equity, portfolio_turnover,
                        drawdown_recovery, estimated_capacity,
                        lowest_capacity_asset, avg_win, avg_loss,
                        backtest_run_start, backtest_run_end,
                        code_hash, runtime_seconds, project_id
                    ) VALUES (
                        ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,
                        ?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,
                        ?23,?24,?25,?26,?27,?28,?29,?30,?31,?32,
                        ?33,?34,?35,?36,?37,?38,?39,?40,?41,?42
                    )",
                    rusqlite::params![
                        exp.name,
                        exp.backtest_id,
                        exp.batch,
                        exp.status,
                        exp.description,
                        exp.hypothesis,
                        exp.based_on,
                        exp.start_date,
                        exp.end_date,
                        exp.cagr,
                        exp.dd,
                        exp.car_mdd,
                        exp.sharpe,
                        exp.sortino,
                        exp.total_orders,
                        exp.win_rate,
                        exp.alpha,
                        exp.beta,
                        exp.expectancy,
                        exp.profit_loss_ratio,
                        exp.annual_std_dev,
                        exp.annual_variance,
                        exp.information_ratio,
                        exp.tracking_error,
                        exp.treynor_ratio,
                        exp.probabilistic_sharpe,
                        exp.net_profit_pct,
                        exp.loss_rate,
                        exp.total_fees,
                        exp.start_equity,
                        exp.end_equity,
                        exp.portfolio_turnover,
                        exp.drawdown_recovery,
                        exp.estimated_capacity,
                        exp.lowest_capacity_asset,
                        exp.avg_win,
                        exp.avg_loss,
                        exp.backtest_run_start,
                        exp.backtest_run_end,
                        exp.code_hash,
                        exp.runtime_seconds,
                        exp.project_id,
                    ],
                )?;
                Ok(())
            })
            .await
    }
}
