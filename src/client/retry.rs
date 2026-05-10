//! Retry policy + transient-error classification for QC API calls.

use anyhow::Result;

/// Retry transient QC API failures (502/503/504, connection errors, timeouts).
/// BOUNDED: 6 retries with back-off [1, 2, 3, 5, 8, 13]s → ~32s max total.
/// Was [2,5,10,15,20,30,45,60,90,120]=397s — too long for hot-path polls
/// (slot cycles appeared stuck for 3+ min while retrying fresh-backtest 502s).
/// Client errors (4xx other than 408/429) fail fast without retry.
pub(crate) async fn with_retry<T, F, Fut>(path: &str, mut op: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let delays = [1u64, 2, 3, 5, 8, 13];
    let mut last_err: Option<anyhow::Error> = None;
    for (attempt, delay) in std::iter::once(&0u64).chain(delays.iter()).enumerate() {
        if *delay > 0 {
            tokio::time::sleep(std::time::Duration::from_secs(*delay)).await;
        }
        match op().await {
            Ok(v) => {
                if attempt > 0 {
                    tracing::info!("qc_api {path} recovered on attempt {attempt}");
                }
                return Ok(v);
            }
            Err(e) => {
                if !is_transient(&e) {
                    return Err(e);
                }
                if attempt < delays.len() {
                    tracing::warn!(
                        "qc_api {path} transient error (attempt {}/{}): {e}",
                        attempt + 1,
                        delays.len() + 1
                    );
                }
                last_err = Some(e);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("with_retry: unknown failure")))
}

/// Classify an anyhow error as transient (retriable) vs permanent (fail fast).
/// Transient: 502, 503, 504, connection errors, timeouts, decode errors on partial responses.
fn is_transient(e: &anyhow::Error) -> bool {
    // Walk the error chain looking for a reqwest error we recognize.
    for cause in e.chain() {
        if let Some(re) = cause.downcast_ref::<reqwest::Error>() {
            if let Some(status) = re.status() {
                return matches!(status.as_u16(), 500 | 502 | 503 | 504 | 408 | 429);
            }
            if re.is_timeout() || re.is_connect() || re.is_request() || re.is_body() {
                return true;
            }
        }
    }
    // Also treat raw "502 Bad Gateway" / "Bad Gateway" / "Gateway Time-out" strings as transient
    // in case reqwest has already been unwrapped into a display string upstream.
    let s = e.to_string();
    s.contains("502")
        || s.contains("503")
        || s.contains("504")
        || s.contains("Bad Gateway")
        || s.contains("Gateway Time")
        || s.contains("timed out")
        || s.contains("connection reset")
        || s.contains("connection closed")
}
