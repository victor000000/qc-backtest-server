//! CLI handler for `delete-projects` command.
//!
//! Trims the project pool down to the first `keep` IDs in `projects.json`
//! (optionally sorted), deleting all remaining IDs from the QC cloud.
//! Mirrors `create-projects` in style.

mod run;
mod sort;

use std::path::Path;

use self::run::{confirm_delete, delete_each_and_save};
use self::sort::sort_by_created;

#[derive(Debug, Clone, Copy)]
pub enum SortBy {
    /// File order — preserve `projects.json` order.
    None,
    /// Sort by `created` timestamp from QC API ascending (oldest first).
    /// `keep` then preserves the OLDEST N, deletes the NEWEST.
    CreatedAsc,
    /// Sort by `created` timestamp descending (newest first).
    /// `keep` preserves the NEWEST N, deletes the OLDEST.
    CreatedDesc,
}

impl std::str::FromStr for SortBy {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "none" | "" => Ok(Self::None),
            "created" | "created_asc" => Ok(Self::CreatedAsc),
            "created_desc" => Ok(Self::CreatedDesc),
            other => Err(format!(
                "invalid sort-by '{other}': use one of none|created|created_desc"
            )),
        }
    }
}

/// Delete cloud projects beyond the first `keep` in `projects.json`.
///
/// `assume_yes` skips the confirmation prompt for non-dry-run.
/// `sort_by` reorders the project list before slicing into keep/delete.
///
/// Failures are preserved in `projects.json` so they can be retried — the
/// new file is `kept ++ failed_deletes`, never silently dropped.
///
/// # Errors
/// Returns `Err` if config loading, file IO, JSON (de)serialization,
/// or stdin read (for confirmation) fails.
pub async fn delete_projects(
    data_dir: &Path,
    keep: usize,
    dry_run: bool,
    assume_yes: bool,
    sort_by: SortBy,
) -> anyhow::Result<()> {
    let projects_path = data_dir.join("projects.json");
    if !projects_path.exists() {
        anyhow::bail!("projects.json not found at {}", projects_path.display());
    }
    let (config, _dbs) = crate::config::ServerConfig::load(data_dir).await?;
    let client = crate::client::QcClient::new(&config.qc_user_id, &config.qc_api_token);
    let content = std::fs::read_to_string(&projects_path)?;
    let parsed: serde_json::Value = serde_json::from_str(&content)?;
    let mut all_pids: Vec<i64> = parsed["projects"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("projects.json: \"projects\" is not an array"))?
        .iter()
        .filter_map(serde_json::Value::as_i64)
        .collect();

    // Optional sort: fetch metadata and reorder by `created` timestamp.
    if !matches!(sort_by, SortBy::None) {
        all_pids = sort_by_created(&client, all_pids, sort_by).await?;
    }

    if keep >= all_pids.len() {
        println!(
            "Keep ({keep}) >= total ({}); nothing to delete.",
            all_pids.len()
        );
        return Ok(());
    }

    let (kept, to_delete) = all_pids.split_at(keep);
    println!(
        "Total: {}; keep: {}; delete: {} (sort_by={:?})",
        all_pids.len(),
        kept.len(),
        to_delete.len(),
        sort_by,
    );

    if dry_run {
        println!("DRY-RUN — would delete:");
        for pid in to_delete {
            println!("  {pid}");
        }
        return Ok(());
    }

    if !assume_yes && !confirm_delete(to_delete.len())? {
        println!("Aborted.");
        return Ok(());
    }

    delete_each_and_save(&client, &projects_path, kept, to_delete).await
}
