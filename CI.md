# Validation

CI builds the committed Cargo.lock with Rust 1.95.0, checks formatting and Clippy, runs local unit tests, and builds release binaries. These checks use no brokerage credentials and do not start trading services. Dependency installation needs ordinary package-registry access. Native/account/data-service integrations require a separately authorized environment and are not certified by this workflow.

Run locally: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`, `cargo test --workspace --all-targets --all-features --locked`, and `cargo build --workspace --release --locked`.

No deployment target or continuous deployment is configured. A successful build is not a deployment or trading-readiness claim.

The legacy repository currently contains no Rust unit tests (cargo test reports zero). CI additionally executes the built CLI help path; this proves startup/parsing without opening an account or starting a server. Behavioral account tests remain unavailable here.

Credential authentication now uses QC_USER_ID/QC_API_TOKEN or a mode-0600 private JSON file selected by QC_CREDENTIALS_FILE (default `.local/credentials.json` beside data). Database credential fallback is removed. The file uses qc_user_id and qc_api_token keys. A regression verifies missing files, private permissions, successful loading and secret-safe malformed-file errors. Organization ID remains ordinary database configuration. No runtime database exists in this isolated legacy checkout; the operational qc_work database migration is documented separately.
