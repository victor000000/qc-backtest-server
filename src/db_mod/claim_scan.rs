//! Candidate scanning: find non-conflicting job from candidates.

/// Result of scanning candidates for a claimable job.
pub(crate) struct ScanResult {
    pub job_id: Option<i64>,
    pub effective_project_id: i64,
    pub skipped: usize,
}

/// Scan candidates and find the first non-conflicting job.
pub(crate) fn scan_candidates(
    candidates: &[(i64, Option<i64>)],
    running_projects: &[i64],
    default_project_id: i64,
    pool: &[i64],
) -> ScanResult {
    let mut job_id: Option<i64> = None;
    let mut effective_project_id = default_project_id;
    let mut skipped = 0usize;

    for (cid, stored_pid) in candidates {
        if let Some(sp) = stored_pid {
            // Job has a pinned project -- check if it conflicts
            if running_projects.contains(sp) {
                skipped += 1;
                continue;
            }
            job_id = Some(*cid);
            effective_project_id = *sp;
            break;
        }
        // No pinned project -- try default first, then scan pool
        if !running_projects.contains(&default_project_id) {
            job_id = Some(*cid);
            effective_project_id = default_project_id;
            break;
        }
        // Default busy -- find any free project from pool
        let mut found_alt = false;
        for alt in pool {
            if !running_projects.contains(alt) {
                job_id = Some(*cid);
                effective_project_id = *alt;
                found_alt = true;
                break;
            }
        }
        if found_alt {
            break;
        }
        // All projects busy -- skip this candidate
        skipped += 1;
    }

    ScanResult {
        job_id,
        effective_project_id,
        skipped,
    }
}
