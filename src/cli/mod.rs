//! Command-line interface root.

mod commands;

pub use commands::Command;

use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
#[command(name = "qc-backtest-server", version)]
pub struct Cli {
    /// Path to the data directory containing strategy databases (S*.db).
    #[arg(long, default_value = "data")]
    pub data_dir: PathBuf,

    /// Directory for log files. Defaults to `<data_dir>/logs` if unset.
    #[arg(long)]
    pub log_dir: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}
