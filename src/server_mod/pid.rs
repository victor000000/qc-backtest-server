//! PID file management — stop existing servers, acquire lock, RAII cleanup.

use std::path::{Path, PathBuf};

use anyhow::Result;

/// On startup, stop any existing server (via PID file + pgrep fallback).
pub(crate) fn stop_existing_server(pid_path: &Path) {
    let my_pid = std::process::id();

    // 1. Try PID file first (reliable)
    if pid_path.exists() {
        if let Ok(content) = std::fs::read_to_string(pid_path)
            && let Ok(old_pid) = content.trim().parse::<u32>()
            && old_pid != my_pid
            && std::path::Path::new(&format!("/proc/{old_pid}")).exists()
        {
            tracing::info!("stopping existing server (PID {old_pid})...");

            // Graceful SIGTERM first
            let _ = std::process::Command::new("kill")
                .args(["-15", &old_pid.to_string()])
                .output();

            // Wait up to 5s for graceful shutdown
            for _ in 0..50 {
                if !std::path::Path::new(&format!("/proc/{old_pid}")).exists() {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }

            // Force kill if still alive
            if std::path::Path::new(&format!("/proc/{old_pid}")).exists() {
                tracing::warn!("PID {old_pid} didn't stop gracefully, force killing");
                let _ = std::process::Command::new("kill")
                    .args(["-9", &old_pid.to_string()])
                    .output();
                std::thread::sleep(std::time::Duration::from_millis(500));
            }

            tracing::info!("existing server stopped");
        }
        let _ = std::fs::remove_file(pid_path);
    }

    // 2. Fallback: pgrep for any orphans not tracked by PID file
    let output = std::process::Command::new("pgrep")
        .args(["-f", "qc-backtest-server serve"])
        .output();

    if let Ok(out) = output {
        let orphans: Vec<u32> = String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter_map(|l| l.trim().parse::<u32>().ok())
            .filter(|&pid| pid != my_pid)
            .collect();

        if !orphans.is_empty() {
            tracing::warn!(
                "killing {} orphan process(es): {:?}",
                orphans.len(),
                orphans
            );
            for pid in &orphans {
                let _ = std::process::Command::new("kill")
                    .args(["-9", &pid.to_string()])
                    .output();
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
    }
}

/// Public for stress test verification.
///
/// # Errors
/// Returns `Err` if another live server owns the PID file or the PID file
/// cannot be written.
pub fn try_acquire_pid_lock(pid_path: &Path) -> Result<()> {
    acquire_pid_lock(pid_path)
}

pub(crate) fn acquire_pid_lock(pid_path: &Path) -> Result<()> {
    if pid_path.exists() {
        let old_pid = std::fs::read_to_string(pid_path)?;
        let old_pid = old_pid.trim();

        if !old_pid.is_empty() {
            let alive = std::path::Path::new(&format!("/proc/{old_pid}")).exists();
            if alive {
                anyhow::bail!(
                    "another server is running (PID {old_pid}). \
                     Remove {} if stale.",
                    pid_path.display()
                );
            }
            tracing::warn!("stale PID file (PID {old_pid} is dead), taking over");
        }
    }

    let my_pid = std::process::id();
    std::fs::write(pid_path, my_pid.to_string())?;
    tracing::info!("PID lock acquired: {my_pid}");
    Ok(())
}

/// RAII guard that removes the PID file on drop.
pub(crate) struct PidGuard(pub PathBuf);

impl Drop for PidGuard {
    fn drop(&mut self) {
        if let Ok(content) = std::fs::read_to_string(&self.0)
            && content.trim() == std::process::id().to_string()
        {
            let _ = std::fs::remove_file(&self.0);
        }
    }
}
