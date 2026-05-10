//! CLI handler for `reconcile-projects` command.
//!
//! Detects drift between `projects.json` and the user's actual QC project
//! list. Reports orphans (IDs locally tracked but missing from QC) and
//! missing (IDs in QC but not tracked locally).

use std::collections::HashSet;
use std::path::Path;

/// Reconcile `projects.json` against QC's project list.
///
/// `prune_orphans=true` rewrites `projects.json` with orphans removed.
///
/// # Errors
/// Returns `Err` if config loading, file IO, or QC API calls fail.
pub async fn reconcile_projects(data_dir: &Path, prune_orphans: bool) -> anyhow::Result<()> {
    let projects_path = data_dir.join("projects.json");
    if !projects_path.exists() {
        anyhow::bail!("projects.json not found at {}", projects_path.display());
    }
    let (config, _dbs) = crate::config::ServerConfig::load(data_dir).await?;
    let client = crate::client::QcClient::new(&config.qc_user_id, &config.qc_api_token);

    // Local set
    let content = std::fs::read_to_string(&projects_path)?;
    let parsed: serde_json::Value = serde_json::from_str(&content)?;
    let local_pids: Vec<i64> = parsed["projects"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("projects.json: \"projects\" is not an array"))?
        .iter()
        .filter_map(serde_json::Value::as_i64)
        .collect();
    let local_set: HashSet<i64> = local_pids.iter().copied().collect();

    // QC set
    let resp = client.read_project(None, None, None).await?;
    if !resp.success {
        anyhow::bail!(
            "read_project failed: {}",
            if resp.errors.is_empty() {
                "(no message)".into()
            } else {
                resp.errors.join("; ")
            }
        );
    }
    let qc_set: HashSet<i64> = resp.projects.iter().map(|p| p.project_id).collect();

    // Compute diffs
    let mut orphans: Vec<i64> = local_set.difference(&qc_set).copied().collect();
    let mut missing: Vec<i64> = qc_set.difference(&local_set).copied().collect();
    orphans.sort_unstable();
    missing.sort_unstable();

    println!("Local projects.json: {} IDs", local_pids.len());
    println!("QC account:          {} IDs", qc_set.len());
    println!("Orphans (in file, not in QC): {}", orphans.len());
    for pid in &orphans {
        println!("  {pid}");
    }
    println!("Missing (in QC, not in file): {}", missing.len());
    for pid in &missing {
        // Annotate with name + created if we have it
        let info = resp.projects.iter().find(|p| p.project_id == *pid);
        match info {
            Some(p) => println!("  {pid}  name={} created={}", p.name, p.created),
            None => println!("  {pid}"),
        }
    }

    if prune_orphans && !orphans.is_empty() {
        let orphan_set: HashSet<i64> = orphans.iter().copied().collect();
        let pruned: Vec<i64> = local_pids
            .into_iter()
            .filter(|p| !orphan_set.contains(p))
            .collect();
        let json = serde_json::json!({"projects": pruned});
        let tmp_path = projects_path.with_extension("json.tmp");
        std::fs::write(&tmp_path, serde_json::to_string_pretty(&json)?)?;
        std::fs::rename(&tmp_path, &projects_path)?;
        println!(
            "Pruned {} orphans from projects.json ({} remaining).",
            orphans.len(),
            pruned.len()
        );
    } else if prune_orphans {
        println!("No orphans to prune.");
    }
    Ok(())
}
