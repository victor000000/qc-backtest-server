//! Dispatch handlers for `queue::*` subcommands (Queue, Cancel, List, ...).

use std::path::Path;

use crate::cli::Command;
use crate::queue;

use super::read_code::read_code;

/// Returns `Some(())` if the command was a queue subcommand and was handled,
/// or `None` if the caller should continue dispatching to other handler groups.
#[allow(
    clippy::too_many_lines,
    reason = "flat match over queue subcommands; splitting hides the dispatch table"
)]
pub(super) async fn try_dispatch(
    command: Command,
    data_dir: &Path,
) -> anyhow::Result<Option<Command>> {
    match command {
        Command::Queue {
            strategy,
            name,
            code_file,
            code_stdin,
            batch,
            priority,
            description,
            hypothesis,
            based_on,
            project_id,
        } => {
            let code = read_code(code_file.as_ref(), code_stdin)?;
            queue::enqueue(queue::QueueCmdInput {
                data_dir,
                strategy: &strategy,
                name: &name,
                code: &code,
                batch: batch.as_deref(),
                priority,
                description: description.as_deref(),
                hypothesis: hypothesis.as_deref(),
                based_on: based_on.as_deref(),
                project_id,
            })
            .await?;
            Ok(None)
        }
        Command::Cancel { strategy, name } => {
            queue::cancel(data_dir, &strategy, &name).await?;
            Ok(None)
        }
        Command::CancelAll { strategy, batch } => {
            queue::cancel_all(data_dir, &strategy, batch.as_deref()).await?;
            Ok(None)
        }
        Command::List {
            strategy,
            status,
            batch,
            name_like,
            limit,
        } => {
            queue::list(
                data_dir,
                strategy.as_deref(),
                &status,
                batch.as_deref(),
                name_like.as_deref(),
                limit,
            )
            .await?;
            Ok(None)
        }
        Command::Errors {
            strategy,
            limit,
            detail,
        } => {
            queue::errors(data_dir, &strategy, limit, detail.as_deref()).await?;
            Ok(None)
        }
        Command::Top {
            strategy,
            batch,
            limit,
        } => {
            queue::top(data_dir, strategy.as_deref(), batch.as_deref(), limit).await?;
            Ok(None)
        }
        Command::Context { strategy } => {
            queue::context(data_dir, &strategy).await?;
            Ok(None)
        }
        Command::Next { strategy } => {
            queue::next_experiment_number(data_dir, &strategy).await?;
            Ok(None)
        }
        Command::Show {
            strategy,
            name,
            code,
        } => {
            queue::show(data_dir, &strategy, &name, code).await?;
            Ok(None)
        }
        Command::Clean {
            strategy,
            older_than_days,
            include_cancelled,
            dry_run,
        } => {
            queue::clean(
                data_dir,
                strategy.as_deref(),
                older_than_days,
                include_cancelled,
                dry_run,
            )
            .await?;
            Ok(None)
        }
        Command::Retry {
            strategy,
            name,
            batch,
        } => {
            queue::retry(data_dir, &strategy, name.as_deref(), batch.as_deref()).await?;
            Ok(None)
        }
        Command::Status => {
            queue::status(data_dir).await?;
            Ok(None)
        }
        // Not a queue command — pass back for the next handler.
        other => Ok(Some(other)),
    }
}
