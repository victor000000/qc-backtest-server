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

        let cred_db = &dbs[0];
        let credential_path = std::env::var_os("QC_CREDENTIALS_FILE").map_or_else(
            || {
                data_dir
                    .parent()
                    .unwrap_or(data_dir)
                    .join(".local/credentials.json")
            },
            std::path::PathBuf::from,
        );
        let qc_user_id = credential(&credential_path, "QC_USER_ID", "qc_user_id")?;
        let qc_api_token = credential(&credential_path, "QC_API_TOKEN", "qc_api_token")?;
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

fn credential(path: &Path, env: &str, key: &str) -> Result<String> {
    if let Ok(value) = std::env::var(env) {
        anyhow::ensure!(!value.trim().is_empty(), "{env} must not be empty");
        return Ok(value);
    }
    private_credential(path, key)
}

fn private_credential(path: &Path, key: &str) -> Result<String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        anyhow::ensure!(
            std::fs::metadata(path)?
                .permissions()
                .mode()
                .trailing_zeros()
                >= 6,
            "QC credential file must have mode 0600"
        );
    }
    let content = std::fs::read(path)
        .map_err(|_| anyhow::anyhow!("Set QC credentials in environment or private file"))?;
    let values: serde_json::Value = serde_json::from_slice(&content)
        .map_err(|_| anyhow::anyhow!("Invalid QC credential JSON"))?;
    let value = values
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    anyhow::ensure!(!value.trim().is_empty(), "Missing private credential {key}");
    Ok(value.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn private_file_required_and_errors_do_not_echo_values() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.json");
        assert!(private_credential(&path, "qc_api_token").is_err());
        std::fs::write(&path, r#"{"qc_api_token":"test-only-token"}"#).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(private_credential(&path, "qc_api_token").is_err());
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        assert_eq!(
            private_credential(&path, "qc_api_token").unwrap(),
            "test-only-token"
        );
        std::fs::write(&path, "sensitive malformed content").unwrap();
        assert!(
            !private_credential(&path, "qc_api_token")
                .unwrap_err()
                .to_string()
                .contains("sensitive")
        );
    }
}
