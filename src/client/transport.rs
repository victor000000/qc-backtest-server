//! Transport-layer impl: `GET` / `POST` / `PUT` / `DELETE` / multipart with retry + tracing.
//!
//! Lives in its own file so `client/mod.rs` stays focused on struct + `new()`.
//! Every request goes through `with_retry` (transient-error retry) + `traced_call`
//! (per-request `elapsed_ms` log at debug level).

use anyhow::Result;
use reqwest::Method;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::QcClient;
use super::retry::with_retry;
use super::trace::traced_call;

impl QcClient {
    /// POST with JSON body → deserialize JSON response.
    pub(crate) async fn post<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &impl Serialize,
    ) -> Result<T> {
        self.json_request(Method::POST, path, Some(body)).await
    }

    /// GET with query parameters → deserialize JSON response.
    pub(crate) async fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &impl Serialize,
    ) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let mut attempt = 0usize;
        with_retry(path, || {
            attempt += 1;
            let url = url.clone();
            async move {
                traced_call(path, "GET", attempt, || async {
                    let resp = self
                        .http
                        .get(&url)
                        .headers(self.headers()?)
                        .query(query)
                        .send()
                        .await?
                        .error_for_status()?;
                    Ok(resp.json().await?)
                })
                .await
            }
        })
        .await
    }

    /// PUT with JSON body.
    pub(crate) async fn put<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &impl Serialize,
    ) -> Result<T> {
        self.json_request(Method::PUT, path, Some(body)).await
    }

    /// DELETE with JSON body.
    pub(crate) async fn delete_req<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &impl Serialize,
    ) -> Result<T> {
        self.json_request(Method::DELETE, path, Some(body)).await
    }

    /// POST multipart form (for binary uploads). No retry — form body is a single-shot stream.
    pub(crate) async fn post_multipart<T: DeserializeOwned>(
        &self,
        path: &str,
        form: reqwest::multipart::Form,
    ) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        traced_call(path, "POST(multipart)", 1, || async {
            let resp = self
                .http
                .post(&url)
                .headers(self.headers()?)
                .multipart(form)
                .send()
                .await?
                .error_for_status()?;
            Ok(resp.json().await?)
        })
        .await
    }

    /// Generic JSON request helper with retry on transient failures.
    async fn json_request<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&impl Serialize>,
    ) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let mut attempt = 0usize;
        with_retry(path, || {
            attempt += 1;
            let url = url.clone();
            let m = method.clone();
            let m_str: &'static str = match m {
                Method::POST => "POST",
                Method::PUT => "PUT",
                Method::DELETE => "DELETE",
                Method::GET => "GET",
                _ => "?",
            };
            async move {
                traced_call(path, m_str, attempt, || async {
                    let mut req = self.http.request(m, &url).headers(self.headers()?);
                    if let Some(b) = body {
                        req = req.json(b);
                    }
                    let resp = req.send().await?;
                    let status = resp.status();
                    let bytes = resp.bytes().await?;
                    // Per-call status + body-size log to crack QC API behavior.
                    // Enabled with RUST_LOG=qc::api=debug. Watch for:
                    // - status not 2xx → QC backend error (rare, usually rate-limit)
                    // - bytes spike on a usually-small endpoint → unexpected payload
                    tracing::debug!(
                        target: "qc::api",
                        "qc_api_resp path={path} method={m_str} status={} bytes={}",
                        status.as_u16(),
                        bytes.len()
                    );
                    if !status.is_success() {
                        return Err(anyhow::anyhow!(
                            "qc {m_str} {path} HTTP {} body={}",
                            status.as_u16(),
                            String::from_utf8_lossy(&bytes[..bytes.len().min(200)])
                        ));
                    }
                    // 2026-05-07: log a body excerpt when QC returns 200 +
                    // `success:false`. Catches unknown soft-fail modes that
                    // would otherwise just bubble up as a vague bail!() in the
                    // caller (the existing `qc_rate_limit_hit` log only fires
                    // on the "too many"/"slow down" patterns). The signature
                    // `"success":false` is checked with a cheap byte scan.
                    if bytes.len() < 4096 {
                        let needle = br#""success":false"#;
                        if bytes
                            .windows(needle.len())
                            .any(|w| w.eq_ignore_ascii_case(needle))
                        {
                            tracing::debug!(
                                target: "qc::api",
                                "qc_api_softfail path={path} method={m_str} bytes={} body={}",
                                bytes.len(),
                                String::from_utf8_lossy(&bytes[..bytes.len().min(240)])
                            );
                        }
                    }
                    Ok(serde_json::from_slice(&bytes)?)
                })
                .await
            }
        })
        .await
    }
}
