//! Push code to a QC project and compile it.

use std::time::Instant;

use anyhow::{Result, bail};
use tokio::time::{Duration, sleep};

use super::SlotRunner;
use crate::db::QueueJob;
use crate::rate_limit::{is_rate_limit, retry_on_rate_limit};

impl SlotRunner {
    /// Push code to a project and compile it.
    ///
    /// Returns `(compile_id, compile_duration, push_start, compile_start)`.
    /// If `pre_compile_id` is provided, skips push+compile entirely.
    pub(crate) async fn push_and_compile(
        &self,
        job: &QueueJob,
        project_id: i64,
        pre_compile_id: Option<String>,
        compile_poll: Duration,
        compile_timeout: Duration,
    ) -> Result<(String, Duration, Instant, Instant)> {
        let slot = self.slot_id;
        let job_name = &job.name;

        // Fast path: already pre-compiled by warm pool
        if let Some(pcid) = pre_compile_id {
            tracing::debug!("slot-{slot}: [{job_name}] using pre-compiled {pcid}");
            return Ok((pcid, Duration::ZERO, Instant::now(), Instant::now()));
        }

        // Safety: verify project is locked before pushing
        {
            let locked = self.project_lock.lock().await;
            if !locked.contains(&project_id) {
                tracing::error!(
                    "slot-{slot}: BUG — project {project_id} not locked \
                     before push for {job_name}!"
                );
                bail!("project_not_locked");
            }
        }

        // ── Push code ──
        tracing::debug!(
            "slot-{slot}: [{job_name}] pushing {} bytes to project {project_id}",
            job.code.len()
        );
        let push_start = Instant::now();

        let push = retry_on_rate_limit(self.rl_state.as_ref(), 3, || {
            self.client
                .update_file_contents(project_id, "main.py", &job.code)
        })
        .await?;

        if !push.success {
            bail!("push_failed: {:?}", push.errors);
        }
        tracing::debug!(
            "slot-{slot}: [{job_name}] push OK in {:.1}s",
            push_start.elapsed().as_secs_f64()
        );

        // ── Create compile ──
        let compile_start = Instant::now();

        let compile = retry_on_rate_limit(self.rl_state.as_ref(), 3, || {
            self.client.create_compile(project_id)
        })
        .await?;

        if !compile.success {
            bail!("compile_create: {:?}", compile.errors);
        }

        let compile_id = compile.compile_id;
        tracing::debug!("slot-{slot}: [{job_name}] compile created: {compile_id}");

        // ── Poll compile until done ──
        let compile_dur = self
            .poll_compile(
                project_id,
                &compile_id,
                job_name,
                compile_poll,
                compile_timeout,
            )
            .await?;

        Ok((compile_id, compile_dur, push_start, compile_start))
    }

    /// Poll a compile until `BuildSuccess`, `BuildError`, or timeout.
    async fn poll_compile(
        &self,
        project_id: i64,
        compile_id: &str,
        job_name: &str,
        poll_interval: Duration,
        timeout: Duration,
    ) -> Result<Duration> {
        let slot = self.slot_id;
        let start = Instant::now();
        let deadline = Instant::now() + timeout;
        let mut first = true;

        loop {
            // 2026-05-07: read FIRST, then sleep only if still InQueue. Restart12
            // logs showed compile/create:compile/read ≈ 1:1 — compile is almost
            // always done on the first poll. The old `sleep before read` shape
            // wasted ~1s per cycle (compile_poll default) for no benefit.
            if !first {
                sleep(poll_interval).await;
            }
            first = false;

            let r = match self.client.read_compile(project_id, compile_id).await {
                Ok(r) => r,
                Err(e) if is_rate_limit(&e) => {
                    tracing::debug!("slot-{slot}: [{job_name}] rate limited on compile poll");
                    sleep(Duration::from_millis(1400)).await;
                    continue;
                }
                Err(e) => return Err(e),
            };

            match r.state.as_str() {
                "BuildSuccess" => {
                    let dur = start.elapsed();
                    tracing::debug!(
                        "slot-{slot}: [{job_name}] compile OK in {:.1}s",
                        dur.as_secs_f64()
                    );
                    return Ok(dur);
                }
                "BuildError" => {
                    tracing::debug!("slot-{slot}: [{job_name}] compile error: {:?}", r.logs);
                    bail!("compile_error: {:?}", r.logs);
                }
                "InQueue" => {
                    if Instant::now() > deadline {
                        bail!("compile_timeout");
                    }
                    tracing::trace!("slot-{slot}: [{job_name}] compile in queue...");
                }
                other => bail!("compile_unknown_state: {other}"),
            }
        }
    }
}
