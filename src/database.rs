use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow};
use rusqlite::Connection;

/// Async-safe wrapper around a `SQLite` connection.
/// All operations run via `spawn_blocking` so they don't block the tokio runtime.
#[derive(Clone)]
pub struct Db {
    conn: Arc<Mutex<Connection>>,
}

impl Db {
    /// Open a `SQLite` database at `path` with WAL + busy-timeout pragmas.
    ///
    /// # Errors
    /// Returns `Err` if the connection fails to open or pragma setup fails.
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path).with_context(|| format!("open {}", path.display()))?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; \
             PRAGMA busy_timeout=30000; \
             PRAGMA synchronous=NORMAL; \
             PRAGMA wal_autocheckpoint=1000;",
        )?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Run a closure on the connection in a blocking thread.
    ///
    /// # Errors
    /// Returns `Err` if the mutex is poisoned, the `spawn_blocking` task
    /// panics, or the closure returns `Err`.
    pub async fn call<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| anyhow!("db lock: {e}"))?;
            f(&conn)
        })
        .await?
    }

    // ── Settings ─────────────────────────────────────────────────

    /// Read a string setting, returning `None` if absent.
    ///
    /// # Errors
    /// Returns `Err` on SQL execution error.
    pub async fn get_setting(&self, category: &str, key: &str) -> Result<Option<String>> {
        let cat = category.to_string();
        let k = key.to_string();
        self.call(move |conn| {
            let mut stmt =
                conn.prepare("SELECT value FROM settings WHERE category=?1 AND key=?2")?;
            let val = stmt
                .query_row(rusqlite::params![cat, k], |row| row.get(0))
                .ok();
            Ok(val)
        })
        .await
    }

    /// Read a string setting; error if missing.
    ///
    /// # Errors
    /// Returns `Err` on SQL execution error or if the setting is absent.
    pub async fn get_setting_required(&self, category: &str, key: &str) -> Result<String> {
        self.get_setting(category, key)
            .await?
            .ok_or_else(|| anyhow!("missing setting {category}/{key}"))
    }

    /// Upsert a string setting with the current timestamp.
    ///
    /// # Errors
    /// Returns `Err` on SQL execution error.
    pub async fn set_setting(&self, category: &str, key: &str, value: &str) -> Result<()> {
        let cat = category.to_string();
        let k = key.to_string();
        let v = value.to_string();
        self.call(move |conn| {
            conn.execute(
                "INSERT OR REPLACE INTO settings (category, key, value, updated_at)
                 VALUES (?1, ?2, ?3, datetime('now'))",
                rusqlite::params![cat, k, v],
            )?;
            Ok(())
        })
        .await
    }

    /// Generic queue counts from any table with a `status` column.
    ///
    /// # Errors
    /// Returns `Err` on SQL execution error.
    pub async fn queue_counts_table(&self, table: &str) -> Result<Vec<(String, i64)>> {
        let sql = format!("SELECT status, COUNT(*) FROM {table} GROUP BY status");
        self.call(move |conn| {
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }
}
