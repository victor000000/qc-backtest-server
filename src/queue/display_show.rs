//! Show full details for a single experiment.

use std::path::Path;

use anyhow::Result;
use rusqlite::OptionalExtension;

use crate::db::Db;

use super::display_show_queue::{show_code, show_queue_fallback};
use super::{ExperimentDetail, open_db};

/// Show full details for a single experiment.
///
/// # Errors
/// Returns `Err` on SQL execution error or if the strategy database fails to open.
pub async fn show(data_dir: &Path, strategy: &str, name: &str, show_code_flag: bool) -> Result<()> {
    let db = open_db(data_dir, strategy)?;
    let n = name.to_string();

    let exp = fetch_experiment(&db, &n).await?;

    if let Some(detail) = exp {
        print_experiment_detail(strategy, name, detail);
    } else {
        // Fall back to queue table
        let found = show_queue_fallback(&db, strategy, name, &n).await?;
        if !found {
            return Ok(());
        }
    }

    if show_code_flag {
        show_code(&db, &n).await?;
    }

    Ok(())
}

/// Query the experiments table for a single experiment detail row.
async fn fetch_experiment(db: &Db, n: &str) -> Result<Option<ExperimentDetail>> {
    let name = n.to_string();
    let exp: Option<ExperimentDetail> = db
        .call(move |conn| {
            conn.query_row(
                "SELECT status, description, hypothesis,
                        based_on, batch, cagr, dd, car_mdd,
                        sharpe, sortino, total_orders, win_rate,
                        net_profit_pct, probabilistic_sharpe,
                        code_hash, project_id
                 FROM experiments WHERE name=?1",
                [&name],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                        row.get(9)?,
                        row.get(10)?,
                        row.get(11)?,
                        row.get(12)?,
                        row.get(13)?,
                        row.get(14)?,
                        row.get::<_, Option<i64>>(15)?,
                    ))
                },
            )
            .optional()
            .map_err(Into::into)
        })
        .await?;
    Ok(exp)
}

/// Print the full detail block for an experiment (header + metrics).
fn print_experiment_detail(strategy: &str, name: &str, detail: ExperimentDetail) {
    let (
        status,
        desc,
        hyp,
        based,
        batch,
        cagr,
        dd,
        car_mdd,
        sharpe,
        sortino,
        orders,
        wr,
        net_pnl,
        prob_sharpe,
        hash,
        proj_id,
    ) = detail;

    println!("=== {strategy}/{name} ===");
    println!(
        "status: {status}  batch: {}",
        batch.as_deref().unwrap_or("-")
    );
    if let Some(d) = &desc {
        println!("description: {d}");
    }
    if let Some(h) = &hyp {
        println!("hypothesis: {h}");
    }
    if let Some(b) = &based {
        println!("based_on: {b}");
    }
    println!();
    println!("Performance:");
    println!("  CAGR:    {:>8.2}%", cagr.unwrap_or(0.0));
    println!("  DD:      {:>8.2}%", dd.unwrap_or(0.0));
    println!("  CAR/MDD: {:>8.3}", car_mdd.unwrap_or(0.0));
    println!("  Sharpe:  {:>8.3}", sharpe.unwrap_or(0.0));
    println!("  Sortino: {:>8.3}", sortino.unwrap_or(0.0));
    println!("  Orders:  {:>8}", orders.unwrap_or(0));
    println!("  WinRate: {:>8.1}%", wr.unwrap_or(0.0));
    println!("  NetPnL:  {:>8.1}%", net_pnl.unwrap_or(0.0));
    println!("  ProbSR:  {:>8.1}%", prob_sharpe.unwrap_or(0.0));
    if let Some(h) = &hash {
        println!("  Hash:    {h}");
    }
    if let Some(p) = proj_id {
        println!("  Project: {p}");
    }
}
