use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::db::Db;

// Re-export all public items from submodules so callers keep using `queue::enqueue(...)` etc.
mod cmd;
mod cmd_cancel;
mod cmd_manage;
mod display;
mod display_box;
mod display_context;
mod display_context_batch;
mod display_context_meta;
mod display_detail;
mod display_detect;
mod display_fmt;
mod display_gather;
mod display_overhead;
mod display_show;
mod display_show_queue;
mod query;
mod query_results;
mod query_top;

pub use cmd::{QueueCmdInput, enqueue};
pub use cmd_cancel::{cancel, cancel_all};
pub use cmd_manage::{clean, retry};
pub use display::status;
pub use display_context::context;
pub use display_detail::next_experiment_number;
pub use display_show::show;
pub use query::list;
pub use query_results::errors;
pub use query_top::top;

// -- Type aliases for complex DB row tuples (shared across submodules) --

pub(crate) type QueueRow = (String, String, i64, Option<String>, Option<String>);
pub(crate) type ExperimentRow = (String, f64, f64, f64, f64, i64, Option<String>);
pub(crate) type ExperimentDetail = (
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<i64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<String>,
    Option<i64>, // project_id
);
pub(crate) type QueueDetail = (String, Option<String>, Option<String>, Option<String>);

// -- Shared helpers --

pub(crate) fn open_db(data_dir: &Path, strategy: &str) -> Result<Db> {
    let db_file = format!("{}.db", strategy.to_uppercase());
    let path = data_dir.join(&db_file);
    if !path.exists() {
        anyhow::bail!(
            "unknown strategy: {strategy} (no {db_file} in {})",
            data_dir.display()
        );
    }
    Db::open(&path, strategy)
}

pub(crate) fn all_dbs(data_dir: &Path) -> Vec<(String, PathBuf)> {
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
                // Only include DBs with a backtest_queue table
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
                let strategy = name.trim_end_matches(".db").to_lowercase();
                found.push((strategy, path));
            }
        }
    }
    found.sort_by(|a, b| a.0.cmp(&b.0));
    found
}

pub(crate) fn strategy_targets(
    data_dir: &Path,
    strategy: Option<&str>,
) -> Result<Vec<(String, PathBuf)>> {
    match strategy {
        Some(s) => {
            let db_file = format!("{}.db", s.to_uppercase());
            let path = data_dir.join(&db_file);
            if !path.exists() {
                anyhow::bail!("unknown strategy: {s} (no {db_file})");
            }
            Ok(vec![(s.to_string(), path)])
        }
        None => Ok(all_dbs(data_dir)),
    }
}

/// Compute code hash (first 8 bytes of SHA-256, hex-encoded).
pub(crate) fn code_hash(code: &str) -> String {
    use sha2::{Digest, Sha256};
    let h = Sha256::digest(code.as_bytes());
    hex::encode(&h[..8])
}

pub(crate) fn truncate_name(name: &str, max_len: usize) -> String {
    if name.len() <= max_len {
        name.to_string()
    } else {
        format!("{}...", &name[..max_len - 3])
    }
}
