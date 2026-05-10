//! Strategy DB discovery and shared project pool loading.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// Auto-discover strategy DBs: any file matching S*.db in `data_dir`.
/// Only includes DBs that have a `backtest_queue` table.
pub(crate) fn discover_strategy_dbs(data_dir: &Path) -> Vec<(String, PathBuf)> {
    let mut found = Vec::new();
    if let Ok(entries) = std::fs::read_dir(data_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && name.starts_with('S')
                && path
                    .extension()
                    .is_some_and(|e| e.eq_ignore_ascii_case("db"))
                && !name.ends_with(".db.bak")
            {
                // Check that this DB has a backtest_queue table
                if let Ok(conn) = rusqlite::Connection::open(&path) {
                    let has_queue: bool = conn
                            .query_row(
                                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='backtest_queue'",
                                [],
                                |r| r.get::<_, i64>(0),
                            )
                            .is_ok_and(|n| n > 0);
                    if !has_queue {
                        continue;
                    }
                }
                // EXAMPLE.db -> example
                let strategy = name.trim_end_matches(".db").to_lowercase();
                found.push((strategy, path));
            }
        }
    }
    found.sort_by(|a, b| a.0.cmp(&b.0));
    found
}

/// Shared project pool loaded from data/projects.json.
#[derive(Debug, serde::Deserialize)]
pub(crate) struct ProjectsFile {
    pub projects: Vec<i64>,
}

/// Load shared project IDs from projects.json if present.
pub(crate) fn load_shared_projects(data_dir: &Path) -> Result<Vec<i64>> {
    let projects_path = data_dir.join("projects.json");
    if projects_path.exists() {
        let content = std::fs::read_to_string(&projects_path)
            .with_context(|| format!("reading {}", projects_path.display()))?;
        let pf: ProjectsFile = serde_json::from_str(&content)
            .with_context(|| format!("parsing {}", projects_path.display()))?;
        tracing::info!(
            "loaded {} shared project IDs from projects.json: {:?}",
            pf.projects.len(),
            pf.projects
        );
        Ok(pf.projects)
    } else {
        tracing::warn!("no projects.json found, falling back to per-strategy DB config");
        Ok(Vec::new())
    }
}
