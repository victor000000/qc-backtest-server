use std::path::Path;

use anyhow::Result;

use crate::client::QcClient;
use crate::config::ServerConfig;

/// Smoke-test the QC API: auth, list nodes for each strategy, read account.
///
/// # Errors
/// Returns `Err` on config load failure or any QC HTTP call failure.
pub async fn test_api(data_dir: &Path) -> Result<()> {
    let (config, _dbs) = ServerConfig::load(data_dir).await?;
    let client = QcClient::new(&config.qc_user_id, &config.qc_api_token);

    // Auth
    let auth = client.authenticate().await?;
    println!("auth: success={}", auth.success);

    // Nodes
    for strat in &config.strategies {
        let Some(&pid) = strat.project_ids.first() else {
            println!("{}: no project IDs, skipping nodes check", strat.name);
            continue;
        };
        let resp = client.read_project_nodes(pid).await?;
        if !resp.success {
            println!(
                "{} nodes ({pid}): FAILED — {}",
                strat.name,
                if resp.errors.is_empty() {
                    "(no message)".into()
                } else {
                    resp.errors.join("; ")
                }
            );
            continue;
        }
        let Some(nodes) = resp.nodes else {
            println!(
                "{} nodes ({pid}): success=true but no nodes payload",
                strat.name
            );
            continue;
        };
        println!(
            "{} nodes ({pid}): {} backtest, autoSelect={}",
            strat.name,
            nodes.backtest.len(),
            resp.auto_select_node,
        );
        for n in &nodes.backtest {
            println!("  {} sku={} busy={}", n.id, n.sku, n.busy);
        }
    }

    // Account
    let acct = client.read_account().await?;
    println!(
        "account: org={} balance={}",
        acct.organization_id, acct.credit_balance
    );

    Ok(())
}
