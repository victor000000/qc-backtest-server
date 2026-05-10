//! CLI command dispatch — routes parsed commands to handlers.

mod queue_cmds;
mod read_code;

use std::path::Path;

use crate::cli::Command;
use crate::{cmd_create_projects, cmd_delete_projects, cmd_reconcile_projects, server, test_api};

/// Dispatch a parsed CLI `Command` to its handler.
///
/// # Errors
/// Returns `Err` if the invoked subcommand handler fails.
pub async fn dispatch(command: Command, data_dir: &Path) -> anyhow::Result<()> {
    let Some(command) = queue_cmds::try_dispatch(command, data_dir).await? else {
        return Ok(());
    };
    match command {
        Command::Serve => server::serve(data_dir.to_path_buf()).await?,
        Command::TestApi => test_api::test_api(data_dir).await?,
        Command::CreateProjects { count } => {
            cmd_create_projects::create_projects(data_dir, count).await?;
        }
        Command::DeleteProjects {
            keep,
            dry_run,
            yes,
            sort_by,
        } => {
            let parsed_sort: cmd_delete_projects::SortBy =
                sort_by.parse().map_err(|e: String| anyhow::anyhow!(e))?;
            cmd_delete_projects::delete_projects(data_dir, keep, dry_run, yes, parsed_sort).await?;
        }
        Command::ReconcileProjects { prune_orphans } => {
            cmd_reconcile_projects::reconcile_projects(data_dir, prune_orphans).await?;
        }
        // queue subcommands consumed by queue_cmds::try_dispatch above.
        _ => unreachable!("queue_cmds::try_dispatch should have handled this variant"),
    }
    Ok(())
}
