//! `SlotRunner` — the core backtest execution unit.
//!
//! Submodules:
//! - `acquire`: job acquisition from warm pool
//! - `backtest`: push code → compile → create backtest → poll
//! - `collect`: result collection from QC API
//! - `compile`: push + compile steps
//! - `errors`: error handling + non-blocking collect spawn
//! - `lock`: project locking
//! - `pick`: weighted job picking
//! - `poll`: backtest polling
//! - `run_loop`: main loop orchestrator

pub mod acquire;
pub mod backtest;
pub mod collect;
pub mod collect_parse;
pub mod compile;
pub mod errors;
pub mod lock;
pub mod pick;
pub mod pipeline;
pub mod poll;
pub mod poll_create;
pub mod queued_check;
pub mod run_loop;

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::client::QcClient;
use crate::config::StrategyConfig;
use crate::db::Db;
use crate::pool::WarmPool;

/// Global lock tracking which project IDs currently have code pushed
/// but not yet backtested. Prevents two slots from pushing code to
/// the same project and overwriting each other.
pub type ProjectLock = Arc<Mutex<HashSet<i64>>>;

// Re-export so existing `crate::runner::RateLimitState` references keep working.
pub use crate::rate_limit::RateLimitState;

/// A runner slot — processes one backtest at a time.
///
/// Each slot runs an infinite loop:
/// 1. Take a pre-compiled entry from the warm pool (or fall back to own push+compile)
/// 2. Create a backtest on QC using the `compile_id`
/// 3. Poll until the backtest completes
/// 4. Spawn a background collect task (non-blocking)
/// 5. Unlock the project so the warm pool can reuse it
pub struct SlotRunner {
    pub slot_id: usize,
    pub client: Arc<QcClient>,
    pub dbs: Vec<Db>,
    pub strategies: Vec<StrategyConfig>,

    /// If set, only pick jobs whose name starts with this prefix.
    pub name_prefix: Option<String>,

    /// Shared adaptive rate-limit state (persisted across restarts).
    pub rl_state: Option<RateLimitState>,

    /// Session counters for status reporting (optional, None in tests).
    pub session: Option<Arc<crate::server::SessionCounters>>,

    /// Job counter for rotating through all projects (not just `slot_id` % len).
    pub(crate) job_counter: usize,

    /// Global lock: projects with pushed code awaiting backtest. Prevents overwrites.
    pub project_lock: ProjectLock,

    /// Shared warm pool of pre-compiled jobs.
    pub warm_pool: Option<Arc<WarmPool>>,

    /// Total slots in deployment — used for backpressure math.
    pub(crate) num_slots: usize,

    /// Global semaphore limiting concurrent `create_backtest` API calls.
    /// QC rate-limits us with 18s delays when too many slots burst-create
    /// simultaneously after a wave of bt-completions. With permits ≈ 4,
    /// bursts serialize gracefully (each create takes ~1-2s) instead of
    /// triggering rate-limit penalties that leave nodes idle.
    pub(crate) create_semaphore: Option<Arc<tokio::sync::Semaphore>>,
}

/// Bundled parameters for the `SlotRunner::new` constructor.
pub struct SlotRunnerNew {
    pub slot_id: usize,
    pub client: Arc<QcClient>,
    pub dbs: Vec<Db>,
    pub strategies: Vec<StrategyConfig>,
    pub name_prefix: Option<String>,
    pub rl_state: Option<RateLimitState>,
    pub session: Option<Arc<crate::server::SessionCounters>>,
    pub project_lock: ProjectLock,
    pub num_slots: usize,
    pub create_semaphore: Option<Arc<tokio::sync::Semaphore>>,
}

impl SlotRunner {
    #[must_use]
    pub fn new(params: SlotRunnerNew) -> Self {
        let SlotRunnerNew {
            slot_id,
            client,
            dbs,
            strategies,
            name_prefix,
            rl_state,
            session,
            project_lock,
            num_slots,
            create_semaphore,
        } = params;
        Self {
            slot_id,
            client,
            dbs,
            strategies,
            name_prefix,
            rl_state,
            session,
            job_counter: 0,
            project_lock,
            warm_pool: None,
            num_slots,
            create_semaphore,
        }
    }
}
