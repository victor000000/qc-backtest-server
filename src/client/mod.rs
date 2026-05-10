//! `QuantConnect` cloud API client.
//!
//! Submodules:
//! - `auth`: SHA256 + base64 hash auth scheme
//! - `retry`: bounded transient-error retry with backoff
//! - `trace`: per-request debug logs (`path` / `method` / `elapsed_ms` / `attempt`)
//! - `transport`: HTTP method wrappers (GET/POST/PUT/DELETE/multipart)
//!
//! High-level API surface (`create_backtest`, `read_project_nodes`, etc.) is
//! defined under `crate::api::*` as `impl QcClient` blocks.

mod auth;
mod retry;
mod trace;
mod transport;

use anyhow::Result;
use reqwest::Client;

use self::auth::auth_headers;

/// `QuantConnect` cloud API client.
///
/// Authenticates using the hash-based scheme described in QC docs:
///   1. `SHA256("{api_token}:{unix_timestamp}")` → hex string
///   2. Base64-encode `"{user_id}:{hex_hash}"`
///   3. `Authorization: Basic {base64}`  +  `Timestamp: {ts}`
pub struct QcClient {
    pub(super) http: Client,
    pub(super) user_id: String,
    pub(super) api_token: String,
    pub(super) base_url: String,
}

impl QcClient {
    pub fn new(user_id: impl Into<String>, api_token: impl Into<String>) -> Self {
        Self {
            http: Client::new(),
            user_id: user_id.into(),
            api_token: api_token.into(),
            base_url: "https://www.quantconnect.com/api/v2".into(),
        }
    }

    pub(super) fn headers(&self) -> Result<reqwest::header::HeaderMap> {
        auth_headers(&self.user_id, &self.api_token)
    }
}
