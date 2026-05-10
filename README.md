# qc-backtest-server

A Rust backtest orchestrator for [QuantConnect](https://www.quantconnect.com/) cloud nodes. Runs many backtests across many cloud projects in parallel, manages a warm-pool of pre-compiled jobs, throttles itself against the QC API, and stores results in per-strategy SQLite databases.

## What it does

QuantConnect gives each user a fixed pool of backtest nodes (typically 10) and a fixed pool of cloud "projects" (one project = one set of files, but only one backtest at a time per project). To run a high-throughput optimizer you need to:

- **Pool projects** — keep N projects warm with pre-compiled code so a slot can fire `create_backtest` immediately.
- **Pool nodes** — bind one local "slot" per backtest node so the dispatcher saturates all nodes.
- **Spread work across projects** — projects can only run one backtest at a time; locking prevents two slots from picking the same project.
- **Pace API calls** — the QC web API rate-limits aggressively (~30 req/s); a global pacer with adaptive backoff keeps the server below the threshold.
- **Survive restarts** — running jobs are recovered from the DB; in-flight QC backtests are reattached on startup.

The server reads a strategy spec table (`backtest_queue`), pushes the Python code to a free QC project, compiles, runs, polls until done, parses statistics, writes to an `experiments` table, and frees the project. Multiple strategies can share the same project pool, weighted by `scheduler_weight`.

## Pipeline

```
queued ──► WARM-POOL FILLER claims a job, pushes code, compiles, parks (project_id, compile_id) on the in-memory pool
       ──► SLOT pulls warm entry, calls QC create_backtest, writes backtest_id to DB
       ──► async polls QC until done, then writes statistics + frees project
```

See `src/runner/` and `src/pool/` for the implementation.

## Build

```bash
cargo build --release
```

The binary lands at `target/release/qc-backtest-server`.

## Setup

The server expects a per-strategy SQLite database (e.g. `MYSTRAT.db`) in the data directory. Each DB has a `settings` table with credentials and a list of QC project IDs the strategy may use:

```sql
CREATE TABLE settings (category TEXT, key TEXT, value TEXT);

INSERT INTO settings VALUES ('credentials', 'qc_user_id', '<your_user_id>');
INSERT INTO settings VALUES ('credentials', 'qc_api_token', '<your_api_token>');
INSERT INTO settings VALUES ('strategy', 'scheduler_weight', '1.0');
```

Project IDs are listed in a `projects.json` (`{"projects": [12345, 12346, ...]}`) at the data dir root and shared across strategies.

To bootstrap projects:

```bash
./qc-backtest-server create-projects --count 10
```

To run:

```bash
./qc-backtest-server --data-dir ./data serve
```

## Commands

See `USAGE.md` for the full command reference.

## License

MIT
