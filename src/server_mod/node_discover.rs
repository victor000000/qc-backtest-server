//! Node discovery + per-project node-allowed-set broadcast.

use anyhow::Result;

use crate::client::QcClient;
use crate::config::ServerConfig;

/// Discover backtest nodes on the QC cloud (uses `strategies[0]` as anchor).
pub(super) async fn discover_nodes(
    client: &QcClient,
    config: &ServerConfig,
) -> Result<Vec<String>> {
    // Discover backtest nodes. The empty-strategies bail in serve()
    // guarantees strategies[0] exists; project_ids is non-empty because the
    // loader skips strategies with no project IDs.
    let first_project = *config.strategies[0].project_ids.first().ok_or_else(|| {
        anyhow::anyhow!("strategies[0] has no project IDs (should be unreachable)")
    })?;
    let nodes_resp = client.read_project_nodes(first_project).await?;
    if !nodes_resp.success {
        anyhow::bail!(
            "read_project_nodes({first_project}) failed: {}",
            if nodes_resp.errors.is_empty() {
                "no error message returned".to_string()
            } else {
                nodes_resp.errors.join("; ")
            }
        );
    }
    let nodes = nodes_resp.nodes.ok_or_else(|| {
        anyhow::anyhow!("read_project_nodes({first_project}): success=true but no nodes payload")
    })?;
    let bt_nodes: Vec<String> = nodes.backtest.iter().map(|n| n.id.clone()).collect();
    tracing::info!("backtest nodes: {bt_nodes:?}");
    for node in &nodes.backtest {
        tracing::info!(
            "  node {} — sku={} speed={} cpu={} ram={} busy={} active={}",
            node.id,
            node.sku,
            node.speed,
            node.cpu,
            node.ram,
            node.busy,
            node.active
        );
    }
    Ok(bt_nodes)
}

/// INFRA FIX 2026-05-06: Each QC project has its own node-allowed-set.
/// If a project's allowed-set is a subset of the full 10 nodes, QC will
/// only schedule that project's backtests on those nodes — leaving other
/// nodes idle even when there's pending work. We were hitting ~70% node
/// utilization with 2 nodes (BN-76fc / BN-81853) chronically idle because
/// those nodes were not in most projects' allowed-sets.
///
/// Fix: at startup, broadcast the full `bt_nodes` list to every project so
/// QC's scheduler can use any node for any project's backtests.
pub(super) async fn broadcast_nodes_to_projects(
    client: &QcClient,
    config: &ServerConfig,
    bt_nodes: &[String],
) {
    use futures::stream::{self, StreamExt};
    let all_pids: Vec<i64> = config
        .strategies
        .iter()
        .flat_map(|s| s.project_ids.iter().copied())
        .collect();
    tracing::info!(
        "broadcasting {} nodes to {} projects via update_project_nodes (parallel x8)",
        bt_nodes.len(),
        all_pids.len()
    );
    let started = std::time::Instant::now();
    // 2026-05-07: parallelized 8-wide via futures::buffer_unordered.
    // Sequential broadcast was ~104s for 74 projects (1.4s/call); parallel
    // 8-wide drops it to ~13s. Server can start picking up backtests sooner.
    //
    // CRITICAL: Send `nodes=None` to enable autoSelectNode=true. QC API quirk:
    // if `nodes` field is present in the request, autoSelectNode is forced to
    // false regardless of the flag's value. Verified empirically 2026-05-06:
    // only by OMITTING the nodes parameter does QC set autoSelect=true.
    let results: Vec<bool> = stream::iter(all_pids.iter().copied())
        .map(|pid| async move {
            match client.update_project_nodes(pid, None, Some(true)).await {
                Ok(r) if r.success => true,
                Ok(r) => {
                    tracing::warn!("update_project_nodes({pid}) success=false: {:?}", r.errors);
                    false
                }
                Err(e) => {
                    tracing::warn!("update_project_nodes({pid}) failed: {e}");
                    false
                }
            }
        })
        .buffer_unordered(8)
        .collect()
        .await;
    let updated = results.iter().filter(|r| **r).count();
    let failed = results.len() - updated;
    let elapsed_ms = started.elapsed().as_millis();
    tracing::info!(
        "node-broadcast done: {updated}/{} projects updated ({failed} failed) elapsed={elapsed_ms}ms",
        all_pids.len()
    );
}
