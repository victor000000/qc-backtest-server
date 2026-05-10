//! Subcommand variants for the CLI.

use std::path::PathBuf;

use clap::Subcommand;

#[rustfmt::skip]
#[derive(Subcommand)]
pub enum Command {
    /// Run the backtest server.
    Serve,

    /// Enqueue a backtest job.
    Queue {
        #[arg(long)] strategy: String,
        #[arg(long)] name: String,
        #[arg(long)] code_file: Option<PathBuf>,
        #[arg(long, conflicts_with = "code_file")] code_stdin: bool,
        #[arg(long)] batch: Option<String>,
        #[arg(long, default_value = "5")] priority: i64,
        #[arg(long)] description: Option<String>,
        #[arg(long)] hypothesis: Option<String>,
        #[arg(long)] based_on: Option<String>,
        #[arg(long)] project_id: Option<i64>,
    },

    /// Cancel a queued job.
    Cancel {
        #[arg(long)] strategy: String,
        #[arg(long)] name: String,
    },

    /// Cancel all queued jobs for a strategy (or a batch).
    CancelAll {
        #[arg(long)] strategy: String,
        #[arg(long)] batch: Option<String>,
    },

    /// List queued/running jobs for a strategy.
    List {
        #[arg(long)] strategy: Option<String>,
        #[arg(long, default_value = "queued,running")] status: String,
        #[arg(long)] batch: Option<String>,
        #[arg(long)] name_like: Option<String>,
        #[arg(long, default_value = "20")] limit: usize,
    },

    /// Show recent errors for a strategy.
    Errors {
        #[arg(long)] strategy: String,
        #[arg(long, default_value = "10")] limit: usize,
        #[arg(long)] detail: Option<String>,
    },

    /// Show top experiments by CAR/MDD.
    Top {
        #[arg(long)] strategy: Option<String>,
        #[arg(long)] batch: Option<String>,
        #[arg(long, default_value = "20")] limit: usize,
    },

    /// Show full optimizer context.
    Context {
        #[arg(long)] strategy: String,
    },

    /// Get the next experiment number for a strategy.
    Next {
        #[arg(long)] strategy: String,
    },

    /// Show full details for a single experiment.
    Show {
        #[arg(long)] strategy: String,
        #[arg(long)] name: String,
        #[arg(long)] code: bool,
    },

    /// Remove old done/failed/cancelled queue entries.
    Clean {
        #[arg(long)] strategy: Option<String>,
        #[arg(long, default_value = "7")] older_than_days: i64,
        #[arg(long)] include_cancelled: bool,
        #[arg(long)] dry_run: bool,
    },

    /// Retry failed jobs (reset to queued).
    Retry {
        #[arg(long)] strategy: String,
        #[arg(long)] name: Option<String>,
        #[arg(long)] batch: Option<String>,
    },

    /// Show queue status for all strategies.
    Status,

    /// Test QC API connectivity.
    TestApi,

    /// Create new QC projects, add to projects.json.
    CreateProjects {
        #[arg(long, default_value = "5")] count: usize,
    },

    /// Delete QC projects beyond the first --keep IDs in projects.json.
    /// Failed deletes are preserved in projects.json. Requires --yes for
    /// non-dry-run.
    DeleteProjects {
        /// Number of project IDs to keep (from the start of projects.json,
        /// after applying --sort-by).
        #[arg(long)] keep: usize,
        /// Print the IDs that would be deleted but make no API calls or file changes.
        #[arg(long)] dry_run: bool,
        /// Skip the confirmation prompt for non-dry-run.
        #[arg(long)] yes: bool,
        /// Reorder projects.json before slicing into keep/delete.
        /// Values: `none` (file order), `created` / `created_asc` (oldest
        /// first → keep oldest), `created_desc` (newest first → keep newest).
        #[arg(long, default_value = "none")] sort_by: String,
    },

    /// Reconcile projects.json against the user's actual QC project list.
    /// Reports orphans (in file, missing on QC) and missing (on QC, not in file).
    /// Use --prune-orphans to remove orphan IDs from projects.json.
    ReconcileProjects {
        /// Remove orphan IDs (in file but missing on QC) from projects.json.
        #[arg(long)] prune_orphans: bool,
    },
}
