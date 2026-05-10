//! Slot runner launch entrypoint.

use std::sync::Arc;

use crate::client::QcClient;
use crate::config::ServerConfig;
use crate::db::Db;
use crate::rate_limit::RateLimitState;
use crate::runner::{SlotRunner, SlotRunnerNew};
use crate::server::SessionCounters;

use super::qc_poller::spawn_qc_poller;
use super::snapshot::spawn_snapshot_loop;
use super::status_loop::spawn_status_loop;
use super::warm::build_warm_pool;

/// Bundled parameters for `launch_slots`.
pub(crate) struct LaunchSlotsParams<'a> {
    pub config: &'a mut ServerConfig,
    pub dbs: &'a [Db],
    pub client: Arc<QcClient>,
    pub bt_nodes: &'a [String],
    pub num_slots: usize,
    pub rl_state: RateLimitState,
    pub session: Arc<SessionCounters>,
    pub project_lock: crate::runner::ProjectLock,
}

/// Launch slot runners and the warm pool, then wait forever.
pub(crate) async fn launch_slots(params: LaunchSlotsParams<'_>) {
    let LaunchSlotsParams {
        config,
        dbs,
        client,
        bt_nodes,
        num_slots,
        rl_state,
        session,
        project_lock,
    } = params;

    let warm = build_warm_pool(config, dbs, &client, num_slots, &rl_state, &project_lock);

    let handles = spawn_slot_runners(&SpawnSlotsArgs {
        client: &client,
        dbs,
        config,
        rl_state: &rl_state,
        session: &session,
        project_lock: &project_lock,
        warm_pool: &warm.pool,
        create_semaphore: &warm.create_semaphore,
        num_slots,
        bt_nodes,
    });

    tracing::info!("server running with {num_slots} slots");

    let status_project = config
        .strategies
        .first()
        .and_then(|s| s.project_ids.first().copied())
        .unwrap_or(0);
    let status_handle = spawn_status_loop(
        dbs.to_vec(),
        rl_state.clone(),
        Arc::clone(&session),
        Arc::clone(&client),
        num_slots,
        status_project,
    );
    let snap_handle = spawn_snapshot_loop(
        Arc::clone(&warm.pool),
        Arc::clone(&project_lock),
        dbs.to_vec(),
        Arc::clone(&session),
        num_slots,
    );
    drop(snap_handle);
    let qc_handle = spawn_qc_poller(Arc::clone(&client), Arc::clone(&session), status_project);
    drop(qc_handle);

    // Wait forever (slots run their own loops)
    for h in handles {
        let _ = h.await;
    }
    status_handle.abort();
    warm.handle.abort();
    // Final save on shutdown
    rl_state.save(&dbs[0]).await;
}

struct SpawnSlotsArgs<'a> {
    client: &'a Arc<QcClient>,
    dbs: &'a [Db],
    config: &'a ServerConfig,
    rl_state: &'a RateLimitState,
    session: &'a Arc<SessionCounters>,
    project_lock: &'a crate::runner::ProjectLock,
    warm_pool: &'a Arc<crate::pool::WarmPool>,
    create_semaphore: &'a Arc<tokio::sync::Semaphore>,
    num_slots: usize,
    bt_nodes: &'a [String],
}

fn spawn_slot_runners(a: &SpawnSlotsArgs<'_>) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::new();
    for slot_id in 0..a.num_slots {
        let mut runner = SlotRunner::new(SlotRunnerNew {
            slot_id,
            client: Arc::clone(a.client),
            dbs: a.dbs.to_vec(),
            strategies: a.config.strategies.clone(),
            name_prefix: None,
            rl_state: Some(a.rl_state.clone()),
            session: Some(Arc::clone(a.session)),
            project_lock: Arc::clone(a.project_lock),
            num_slots: a.num_slots,
            create_semaphore: Some(Arc::clone(a.create_semaphore)),
        });
        runner.warm_pool = Some(Arc::clone(a.warm_pool));
        tracing::info!(
            "starting slot-{slot_id} (node={})",
            a.bt_nodes.get(slot_id).map_or("auto", String::as_str)
        );
        handles.push(tokio::spawn(async move { runner.run().await }));
    }
    handles
}
