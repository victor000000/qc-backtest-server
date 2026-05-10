//! Backtest data read API methods: charts, orders, insights, reports.

use anyhow::Result;

use crate::client::QcClient;
use crate::models::backtest_detail::OrdersResponse;
use crate::models::backtest_ext::{BacktestReportResponse, ChartReadResponse};
use crate::models::backtest_insights::InsightsResponse;
use crate::models::backtest_req::{BacktestIdReq, ReadChartReq, ReadInsightsReq, ReadOrdersReq};

impl QcClient {
    /// Read chart data for a backtest.
    ///
    /// # Errors
    /// Returns `Err` on HTTP transport failure or deserialization failure.
    pub async fn read_backtest_chart(
        &self,
        project_id: i64,
        backtest_id: &str,
        chart_name: &str,
        count: i64,
        start: i64,
        end: i64,
    ) -> Result<ChartReadResponse> {
        self.post(
            "/backtests/chart/read",
            &ReadChartReq {
                project_id,
                backtest_id,
                name: chart_name,
                count,
                start,
                end,
            },
        )
        .await
    }

    /// Read orders from a backtest (end − start must be < 100).
    ///
    /// # Errors
    /// Returns `Err` on HTTP transport failure or deserialization failure.
    pub async fn read_backtest_orders(
        &self,
        project_id: i64,
        backtest_id: &str,
        start: i64,
        end: i64,
    ) -> Result<OrdersResponse> {
        self.post(
            "/backtests/orders/read",
            &ReadOrdersReq {
                project_id,
                backtest_id,
                start,
                end,
            },
        )
        .await
    }

    /// Read insights from a backtest (end − start must be < 100).
    ///
    /// # Errors
    /// Returns `Err` on HTTP transport failure or deserialization failure.
    pub async fn read_backtest_insights(
        &self,
        project_id: i64,
        backtest_id: &str,
        start: i64,
        end: i64,
    ) -> Result<InsightsResponse> {
        self.post(
            "/backtests/read/insights",
            &ReadInsightsReq {
                project_id,
                backtest_id,
                start,
                end,
            },
        )
        .await
    }

    /// Request a backtest report (HTML with embedded images).
    ///
    /// # Errors
    /// Returns `Err` on HTTP transport failure or deserialization failure.
    pub async fn read_backtest_report(
        &self,
        project_id: i64,
        backtest_id: &str,
    ) -> Result<BacktestReportResponse> {
        self.post(
            "/backtests/read/report",
            &BacktestIdReq {
                project_id,
                backtest_id,
            },
        )
        .await
    }
}
