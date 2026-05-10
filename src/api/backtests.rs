use std::collections::HashMap;

use anyhow::Result;
use serde_json::Value;

use crate::client::QcClient;
use crate::models::backtest::BacktestResponse;
use crate::models::backtest_list::BacktestListResponse;
use crate::models::backtest_req::{
    BacktestIdReq, CreateBacktestReq, ListBacktestsReq, UpdateBacktestReq, UpdateBacktestTagsReq,
};
use crate::models::common::RestResponse;

impl QcClient {
    // ── Backtest CRUD ────────────────────────────────────────────

    /// Create and launch a new cloud backtest.
    ///
    /// # Errors
    /// Returns `Err` on HTTP transport failure or deserialization failure.
    pub async fn create_backtest(
        &self,
        project_id: i64,
        compile_id: &str,
        backtest_name: &str,
        parameters: Option<&HashMap<String, Value>>,
    ) -> Result<BacktestResponse> {
        self.post(
            "/backtests/create",
            &CreateBacktestReq {
                project_id,
                compile_id,
                backtest_name,
                parameters,
            },
        )
        .await
    }

    /// Read full backtest results (statistics, charts, etc.).
    ///
    /// # Errors
    /// Returns `Err` on HTTP transport failure or deserialization failure.
    pub async fn read_backtest(
        &self,
        project_id: i64,
        backtest_id: &str,
    ) -> Result<BacktestResponse> {
        self.post(
            "/backtests/read",
            &BacktestIdReq {
                project_id,
                backtest_id,
            },
        )
        .await
    }

    /// List all backtests for a project.
    ///
    /// # Errors
    /// Returns `Err` on HTTP transport failure or deserialization failure.
    pub async fn list_backtests(
        &self,
        project_id: i64,
        include_statistics: bool,
    ) -> Result<BacktestListResponse> {
        self.post(
            "/backtests/list",
            &ListBacktestsReq {
                project_id,
                include_statistics: if include_statistics { Some(true) } else { None },
            },
        )
        .await
    }

    /// Update backtest name / note.
    ///
    /// # Errors
    /// Returns `Err` on HTTP transport failure or non-2xx response.
    pub async fn update_backtest(
        &self,
        project_id: i64,
        backtest_id: &str,
        name: Option<&str>,
        note: Option<&str>,
    ) -> Result<RestResponse> {
        self.post(
            "/backtests/update",
            &UpdateBacktestReq {
                project_id,
                backtest_id,
                name,
                note,
            },
        )
        .await
    }

    /// Delete a backtest.
    ///
    /// # Errors
    /// Returns `Err` on HTTP transport failure or non-2xx response.
    pub async fn delete_backtest(
        &self,
        project_id: i64,
        backtest_id: &str,
    ) -> Result<RestResponse> {
        self.post(
            "/backtests/delete",
            &BacktestIdReq {
                project_id,
                backtest_id,
            },
        )
        .await
    }

    /// Update tags on a backtest.
    ///
    /// # Errors
    /// Returns `Err` on HTTP transport failure or non-2xx response.
    pub async fn update_backtest_tags(
        &self,
        project_id: i64,
        backtest_id: &str,
        tags: &[String],
    ) -> Result<RestResponse> {
        self.post(
            "/backtests/tags/update",
            &UpdateBacktestTagsReq {
                project_id,
                backtest_id,
                tags,
            },
        )
        .await
    }

    // Data read methods (chart, orders, insights, report) are in backtests_data.rs
}
