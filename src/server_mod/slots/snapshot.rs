//! Lightweight pool/lock-state snapshot loop (15s cadence) for
//! fast diagnosis of slot starvation.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::task::JoinHandle;
use tokio::time::Duration;

use crate::db::Db;
use crate::server::SessionCounters;

pub(super) fn spawn_snapshot_loop(
    pool: Arc<crate::pool::WarmPool>,
    lock: crate::runner::ProjectLock,
    dbs: Vec<Db>,
    session: Arc<SessionCounters>,
    num_slots: usize,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(15)).await;
            let pool_len = pool.len().await;
            let locked_count = lock.lock().await.len();
            let (queued, prewarm, bt_running, busy) = scan_dbs(&dbs).await;
            // PERF: track starvation — queue has work but slots idle
            if queued > 0 && busy < num_slots {
                let idle = (num_slots - busy) as u64;
                session.starvation_secs.fetch_add(15, Ordering::SeqCst);
                session
                    .idle_slot_secs
                    .fetch_add(idle * 15, Ordering::SeqCst);
                if busy == 0 {
                    tracing::warn!(
                        "FULL STARVATION: queued={queued} but 0/{} slots busy \
                         (pool={pool_len} prewarm={prewarm} bt_running={bt_running})",
                        num_slots
                    );
                } else if num_slots - busy >= num_slots / 2 {
                    tracing::warn!(
                        "starvation: queued={queued} busy={busy}/{} \
                         (pool={pool_len} prewarm={prewarm})",
                        num_slots
                    );
                }
            }
            tracing::debug!(
                "snapshot: pool={pool_len} locked_projects={locked_count} \
                 bt_running={bt_running} prewarm={prewarm} \
                 busy_slots={busy}/{} queued={queued}",
                num_slots
            );
        }
    })
}

/// Scan all DBs and return (queued, prewarm, `bt_running`, `distinct_busy_slots`).
async fn scan_dbs(dbs: &[Db]) -> (i64, i64, i64, usize) {
    let mut bt_running = 0i64;
    let mut prewarm = 0i64;
    let mut queued = 0i64;
    let mut distinct_slots: std::collections::HashSet<String> = std::collections::HashSet::new();
    for db in dbs {
        let (q, r, b, slot_ids): (i64, i64, i64, Vec<String>) = db
            .call(|conn| {
                let q: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM backtest_queue WHERE status='queued'",
                        [],
                        |r| r.get(0),
                    )
                    .unwrap_or(0);
                let r: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM backtest_queue \
                     WHERE status='running' AND backtest_id IS NULL",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap_or(0);
                let b: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM backtest_queue \
                     WHERE status='running' AND backtest_id IS NOT NULL",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap_or(0);
                let mut stmt = conn.prepare(
                    "SELECT DISTINCT node_id FROM backtest_queue \
                     WHERE status='running' AND node_id LIKE 'slot-%'",
                )?;
                let ids: Vec<String> = stmt
                    .query_map([], |row| row.get::<_, String>(0))?
                    .filter_map(std::result::Result::ok)
                    .collect();
                Ok((q, r, b, ids))
            })
            .await
            .unwrap_or((0, 0, 0, Vec::new()));
        queued += q;
        prewarm += r;
        bt_running += b;
        for id in slot_ids {
            distinct_slots.insert(id);
        }
    }
    (queued, prewarm, bt_running, distinct_slots.len())
}
