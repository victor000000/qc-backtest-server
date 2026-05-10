//! Server startup: config load, auth, node discovery, slot launch.

mod node_discover;
pub mod overhead;
pub mod pid;
pub mod recover;
mod session;
pub mod slots;
pub mod status;
mod status_emit;
pub mod status_fmt;
pub(crate) mod status_query;

pub use session::SessionCounters;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;

use self::node_discover::{broadcast_nodes_to_projects, discover_nodes};
use self::slots::{LaunchSlotsParams, launch_slots};
use self::status::recover_running_jobs;
use crate::client::QcClient;
use crate::config::ServerConfig;

const PID_FILE: &str = "qc-server.pid";

/// Start the backtest server: acquire PID lock, load config, discover nodes,
/// launch runner slots.
///
/// # Errors
/// Returns `Err` if the PID lock cannot be acquired, config loading fails,
/// QC authentication fails, or no backtest nodes are available.
pub async fn serve(data_dir: PathBuf) -> Result<()> {
    // -- 0. Stop any existing server cleanly --
    let pid_path = data_dir.join(PID_FILE);
    pid::stop_existing_server(&pid_path);

    // -- 1. PID lock --
    pid::acquire_pid_lock(&pid_path)?;
    let _pid_guard = pid::PidGuard(pid_path.clone());

    tracing::info!("loading config from {}", data_dir.display());
    let (mut config, dbs) = ServerConfig::load(&data_dir).await?;

    if config.strategies.is_empty() {
        anyhow::bail!(
            "no strategies with project IDs found. Add IDs to projects.json \
             (or run `create-projects --count N`) before serving."
        );
    }

    log_strategies(&config);

    let client = Arc::new(QcClient::new(&config.qc_user_id, &config.qc_api_token));

    // Verify auth
    let auth = client.authenticate().await?;
    if !auth.success {
        anyhow::bail!("QC auth failed: {:?}", auth.errors);
    }
    tracing::info!("QC authentication OK");

    let bt_nodes = discover_nodes(&client, &config).await?;
    config.node_ids = bt_nodes.clone();
    broadcast_nodes_to_projects(&client, &config, &bt_nodes).await;

    let num_projects = config.strategies.first().map_or(0, |s| s.project_ids.len());
    let num_slots = bt_nodes.len();
    if num_slots == 0 {
        anyhow::bail!("no backtest nodes available");
    }
    if num_projects == 0 {
        anyhow::bail!("no project IDs configured");
    }
    tracing::info!(
        "launching {num_slots} slot(s) for {} node(s), {} project(s) \
         (slots independent of project count)",
        bt_nodes.len(),
        num_projects
    );

    // -- 2. Recover stale running jobs --
    recover_running_jobs(&dbs, &client).await?;

    let rl_state = crate::rate_limit::RateLimitState::load(&dbs[0]).await;

    // Print queue status
    for db in &dbs {
        let counts = db.queue_counts().await?;
        let summary: String = counts
            .iter()
            .map(|(s, c)| format!("{s}={c}"))
            .collect::<Vec<_>>()
            .join(" ");
        tracing::info!("{} queue: {summary}", db.strategy);
    }

    // -- 3. Launch warm pool + slot runners --
    let session = Arc::new(SessionCounters::default());
    // Sentinel: usize::MAX means "QC busy poll has not run yet — use local fallback"
    session
        .qc_busy_slots
        .store(usize::MAX, std::sync::atomic::Ordering::SeqCst);
    let project_lock = crate::runner::ProjectLock::default();

    launch_slots(LaunchSlotsParams {
        config: &mut config,
        dbs: &dbs,
        client,
        bt_nodes: &bt_nodes,
        num_slots,
        rl_state,
        session,
        project_lock,
    })
    .await;

    Ok(())
}

fn log_strategies(config: &ServerConfig) {
    tracing::info!(
        "strategies: {}",
        config
            .strategies
            .iter()
            .map(|s| {
                let pids = s
                    .project_ids
                    .iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(",");
                format!("{}(w={}, projects=[{pids}])", s.name, s.weight)
            })
            .collect::<Vec<_>>()
            .join(", ")
    );
}
