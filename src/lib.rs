#![warn(clippy::pedantic)]

// ── QC API layer ────────────────────────────────────────────────
pub mod api;
pub mod client;
pub mod models;

// ── Database layer ──────────────────────────────────────────────
pub mod database;
#[path = "db_mod/mod.rs"]
pub mod db;

// ── Server + runner pipeline ────────────────────────────────────
pub mod config;
mod config_discover;
pub mod pool;
pub mod runner;
#[path = "server_mod/mod.rs"]
pub mod server;

// ── CLI + queue commands ────────────────────────────────────────
pub mod cli;
pub mod cmd_create_projects;
pub mod cmd_delete_projects;
pub mod cmd_dispatch;
pub mod cmd_reconcile_projects;
pub mod queue;

// ── Utilities ───────────────────────────────────────────────────
pub mod log_setup;
pub mod rate_limit;
pub mod test_api;
