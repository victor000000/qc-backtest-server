//! CLI handler for `create-projects` command.

use std::path::Path;

/// Maximum consecutive failures before bailing out.
const MAX_CONSECUTIVE_FAILURES: usize = 5;

/// Create `count` new QC projects and append their IDs to `projects.json`.
///
/// Behavior:
/// - Names are `bt_pool_<n>` where `n` is the next free numeric suffix not
///   already in use across the user's QC projects (avoids name collisions
///   when prior `delete-projects` left gaps).
/// - `projects.json` is rewritten after EVERY successful creation so an
///   interruption never loses already-created IDs.
/// - Bails out after `MAX_CONSECUTIVE_FAILURES` consecutive failures
///   (rate limit / outage / auth).
///
/// # Errors
/// Returns `Err` if config loading, file IO, or JSON (de)serialization fails.
pub async fn create_projects(data_dir: &Path, count: usize) -> anyhow::Result<()> {
    let (config, _dbs) = crate::config::ServerConfig::load(data_dir).await?;
    let client = crate::client::QcClient::new(&config.qc_user_id, &config.qc_api_token);
    let projects_path = data_dir.join("projects.json");
    let mut projects = read_projects_json(&projects_path)?;
    println!("Current local projects.json: {}", projects.len());

    // Find next free bt_pool_<n> suffix by querying QC for all the user's
    // projects and scanning their names. Avoids creating duplicates with
    // existing `bt_pool_*` names left from prior runs.
    let next_suffix = compute_next_pool_suffix(&client).await?;
    println!("Next free pool suffix: bt_pool_{next_suffix}");

    let mut consecutive_failures = 0usize;
    let mut current_suffix = next_suffix;
    let mut created = 0usize;

    for i in 0..count {
        let name = format!("bt_pool_{current_suffix}");
        match client.create_project(&name, "Py", None).await {
            Ok(resp) if resp.success => {
                if let Some(p) = resp.projects.first() {
                    println!(
                        "[{}/{count}] Created {name}: project_id={}",
                        i + 1,
                        p.project_id
                    );
                    projects.push(p.project_id);
                    write_projects_json(&projects_path, &projects)?;
                    consecutive_failures = 0;
                    current_suffix += 1;
                    created += 1;
                } else {
                    // success=true with empty projects is a permanent
                    // protocol violation — retrying won't help.
                    anyhow::bail!(
                        "[{}/{count}] {name}: success=true but no project returned — \
                         aborting (created {created} so far)",
                        i + 1
                    );
                }
            }
            Ok(resp) => {
                let msg = if resp.errors.is_empty() {
                    "(no error message)".into()
                } else {
                    resp.errors.join("; ")
                };
                println!("[{}/{count}] FAIL {name}: {msg}", i + 1);
                // Detect QC daily creation limit — retrying is futile until tomorrow.
                if resp.errors.iter().any(|e| {
                    let lc = e.to_ascii_lowercase();
                    lc.contains("maximum number of projects") || lc.contains("daily limit")
                }) {
                    println!(
                        "Hit QC daily creation limit — stopping early (created {created} of {count})."
                    );
                    println!("Done. projects.json: {} total", projects.len());
                    return Ok(());
                }
                consecutive_failures += 1;
                current_suffix += 1;
            }
            Err(e) => {
                println!("[{}/{count}] ERROR {name}: {e}", i + 1);
                consecutive_failures += 1;
                current_suffix += 1;
            }
        }
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
            anyhow::bail!(
                "{MAX_CONSECUTIVE_FAILURES} consecutive failures — \
                 aborting (created {created} of {count})"
            );
        }
        // Polite spacing between API calls (matches delete_each_and_save).
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    println!(
        "Done. projects.json: {} total ({created} new)",
        projects.len()
    );
    Ok(())
}

/// Read the existing `projects.json`. Returns empty vec if file missing.
fn read_projects_json(path: &Path) -> anyhow::Result<Vec<i64>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content = std::fs::read_to_string(path)?;
    let parsed: serde_json::Value = serde_json::from_str(&content)?;
    let arr = parsed["projects"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("projects.json: \"projects\" is not an array"))?;
    Ok(arr.iter().filter_map(serde_json::Value::as_i64).collect())
}

/// Atomically write `projects.json` via tmp-file + rename so an interrupt
/// never leaves a half-written file.
fn write_projects_json(path: &Path, projects: &[i64]) -> anyhow::Result<()> {
    let json = serde_json::json!({"projects": projects});
    let tmp_path = path.with_extension("json.tmp");
    std::fs::write(&tmp_path, serde_json::to_string_pretty(&json)?)?;
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

/// Query QC for all the user's projects and find the lowest unused
/// numeric suffix on `bt_pool_<n>`. Returns 1 if there are no `bt_pool_*`
/// projects on the account.
async fn compute_next_pool_suffix(client: &crate::client::QcClient) -> anyhow::Result<u64> {
    let resp = client.read_project(None, None, None).await?;
    if !resp.success {
        anyhow::bail!(
            "read_project failed: {}",
            if resp.errors.is_empty() {
                "(no message)".into()
            } else {
                resp.errors.join("; ")
            }
        );
    }
    let mut used: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
    for p in &resp.projects {
        if let Some(suffix) = p.name.strip_prefix("bt_pool_")
            && let Ok(n) = suffix.parse::<u64>()
        {
            used.insert(n);
        }
    }
    // First gap (or one-past-max). E.g. used={2,3,4} → 1; used={1,2,4} → 3.
    let mut next = 1u64;
    while used.contains(&next) {
        next += 1;
    }
    Ok(next)
}
