//! Warm pool helpers: push code, compile, poll compile status.

mod compile;
mod syntax;

pub(crate) use compile::{compile_code, poll_compile};
pub(crate) use syntax::local_syntax_check;

use crate::client::QcClient;
use crate::rate_limit::{RateLimitState, retry_on_rate_limit};

pub(crate) async fn push_code(
    client: &QcClient,
    rl_state: Option<&RateLimitState>,
    project_id: i64,
    code: &str,
    job_name: &str,
) -> Result<(), ()> {
    let push_result = retry_on_rate_limit(rl_state, 3, || {
        client.update_file_contents(project_id, "main.py", code)
    })
    .await;

    match push_result {
        Ok(ref p) if p.success => {
            tracing::debug!("warm-pool: pushed {job_name} to project {project_id}");
            Ok(())
        }
        _ => {
            tracing::warn!("warm-pool: push failed for {job_name}, requeuing");
            Err(())
        }
    }
}
