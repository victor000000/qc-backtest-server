use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::config_discover::{discover_strategy_dbs, load_shared_projects};
use crate::db::Db;

/// Per-strategy configuration loaded from the database.
#[derive(Debug, Clone)]
pub struct StrategyConfig {
    pub name: String,
    pub db_path: PathBuf,
    /// QC project IDs -- one per runner slot to avoid code-push conflicts.
    pub project_ids: Vec<i64>,
    /// Scheduler weight (default 1.0). Higher = more likely to be picked.
    pub weight: f64,
}

/// Top-level server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub qc_user_id: String,
    pub qc_api_token: String,
    pub organization_id: String,
    pub strategies: Vec<StrategyConfig>,
    /// Discovered backtest node IDs.
    pub node_ids: Vec<String>,
}

impl ServerConfig {
    /// Load config from all strategy databases found in `data_dir`.
    /// Project IDs are loaded from shared `projects.json` (not per-strategy DBs).
    ///
    /// # Errors
    /// Returns `Err` if no `S*.db` is found, SQL queries fail, or required
    /// credential settings are missing.
    pub async fn load(data_dir: &Path) -> Result<(Self, Vec<Db>)> {
        let mut dbs = Vec::new();
        let mut strategies = Vec::new();

        let shared_project_ids = load_shared_projects(data_dir)?;

        let db_files = discover_strategy_dbs(data_dir);
        if db_files.is_empty() {
            anyhow::bail!(
                "no strategy databases (S*.db) found in {}",
                data_dir.display()
            );
        }

        for (name, path) in &db_files {
            let db = Db::open(path, name)?;

            // Use shared projects.json, fallback to per-DB settings
            let project_ids = if shared_project_ids.is_empty() {
                let mut pids = Vec::new();
                if let Some(v) = db.get_setting("project", "cloud_project_id").await?
                    && let Ok(id) = v.parse::<i64>()
                {
                    pids.push(id);
                }
                for n in 2..=20 {
                    if let Some(v) = db
                        .get_setting("project", &format!("cloud_project_id_{n}"))
                        .await?
                        && let Ok(id) = v.parse::<i64>()
                    {
                        pids.push(id);
                    }
                }
                if pids.is_empty() {
                    // Strategy is disabled (no projects to run on), but its DB
                    // is still kept so admin commands like `create-projects`
                    // and `delete-projects` can read credentials. Without this
                    // push, deleting all projects bricks the bootstrap because
                    // every subsequent CLI hits an empty `dbs` and panics.
                    tracing::warn!(
                        "{name}: no project IDs (strategy disabled, \
                         db kept for credentials)"
                    );
                    dbs.push(db);
                    continue;
                }
                pids
            } else {
                shared_project_ids.clone()
            };

            let weight: f64 = db
                .get_setting("strategy", "scheduler_weight")
                .await?
                .and_then(|v| v.parse().ok())
                .unwrap_or(1.0);

            strategies.push(StrategyConfig {
                name: name.clone(),
                db_path: path.clone(),
                project_ids,
                weight,
            });
            dbs.push(db);
        }

        // Use first DB for shared credentials
        let cred_db = &dbs[0];
        let qc_user_id = cred_db
            .get_setting_required("credentials", "qc_user_id")
            .await?;
        let qc_api_token = cred_db
            .get_setting_required("credentials", "qc_api_token")
            .await?;
        let organization_id = cred_db
            .get_setting_required("project", "organization_id")
            .await?;

        Ok((
            Self {
                qc_user_id,
                qc_api_token,
                organization_id,
                strategies,
                node_ids: Vec::new(),
            },
            dbs,
        ))
    }
}
