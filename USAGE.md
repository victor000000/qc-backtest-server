# qc-backtest-server usage

Binary: `./qc-backtest-server` (or `cargo run --release --`)

Global option: `--data-dir <path>` (default: `data`)

Replace `<strategy>` with the lowercase strategy name (matches the `<STRATEGY>.db` filename).

## serve — run the server

```bash
./qc-backtest-server serve
```

Runs in foreground. Discovers backtest nodes from QC API, launches one slot per node. Kills orphan server processes on startup, recovers stale `running` jobs from previous crashes.

Logs: `data/logs/server.log` (INFO, readable) and `data/logs/server_full.log` (DEBUG). Status box printed every 60s.

## status — show queue status

```bash
./qc-backtest-server status
```

Auto-detects if server is running (via PID file), shows uptime, throughput, rate-limit backoff, per-strategy queue counts, and recent errors.

## context — full optimizer context

```bash
./qc-backtest-server context --strategy <strategy>
```

All-in-one view: optimizer description, strategy params, queue counts, next experiment number, currently running jobs, top 10 experiments, latest batch results, recent failures.

## next — next experiment number

```bash
./qc-backtest-server next --strategy <strategy>
```

Prints the next available experiment number (scans both `experiments` and `backtest_queue` tables).

## queue — enqueue a backtest

```bash
./qc-backtest-server queue \
  --strategy <strategy> \
  --name "Exp100_name" \
  --code-file path/to/strategy.py \
  --batch "batch_label" \
  --priority 5 \
  --description "what changed" \
  --hypothesis "why this experiment" \
  --based-on "Exp99_parent"
```

- `--code-file` or `--code-stdin` (required, mutually exclusive)
- `--priority`: lower = runs first (default 5)
- `--batch`, `--description`, `--hypothesis`, `--based-on`: optional metadata
- Rejects duplicate names; warns on duplicate code hash

## list — list queue entries

```bash
# Queued + running (default)
./qc-backtest-server list --strategy <strategy>

# All strategies, filter by status
./qc-backtest-server list --status done --limit 10

# Filter by batch or name pattern
./qc-backtest-server list --strategy <strategy> --batch "<batch>" --name-like "%pattern%"
```

`--status` accepts comma-separated values: `queued,running,done,failed,cancelled`.

## top — leaderboard

```bash
./qc-backtest-server top --strategy <strategy> --limit 10
./qc-backtest-server top --batch "<batch>"
```

Shows experiments sorted by CAR/MDD descending. Omit `--strategy` for all.

## show — experiment details

```bash
./qc-backtest-server show --strategy <strategy> --name <experiment_name>
./qc-backtest-server show --strategy <strategy> --name <experiment_name> --code
```

Full stats (CAGR, DD, CAR/MDD, Sharpe, Sortino, WinRate, etc). `--code` also prints the stored strategy source.

## errors — recent failures

```bash
./qc-backtest-server errors --strategy <strategy> --limit 5

# Full QC API response for a specific failure
./qc-backtest-server errors --strategy <strategy> --detail <experiment_name>
```

## cancel / cancel-all

```bash
# Cancel one queued job
./qc-backtest-server cancel --strategy <strategy> --name "<experiment_name>"

# Cancel all queued in a strategy
./qc-backtest-server cancel-all --strategy <strategy>

# Cancel only a batch
./qc-backtest-server cancel-all --strategy <strategy> --batch "<batch>"
```

Only affects jobs in `queued` state (not running).

## retry — requeue failed jobs

```bash
# Retry one
./qc-backtest-server retry --strategy <strategy> --name <experiment_name>

# Retry all failures in a batch
./qc-backtest-server retry --strategy <strategy> --batch "<batch>"
```

Resets failed jobs to `queued` (clears error, retry count, result_json).

## clean — remove old queue entries

```bash
# Preview what would be deleted
./qc-backtest-server clean --strategy <strategy> --dry-run

# Delete done/failed entries older than 7 days (default)
./qc-backtest-server clean --older-than-days 7

# Also remove cancelled entries
./qc-backtest-server clean --include-cancelled
```

Omit `--strategy` to clean all.

## test-api / mock-test / stress-test

```bash
./qc-backtest-server test-api       # Auth + node discovery
./qc-backtest-server mock-test      # Real QC API, tiny backtests
./qc-backtest-server stress-test    # Mock QC, chaos/race conditions
```

## create-projects / delete-projects / reconcile-projects

```bash
# Create N new QC projects, append to projects.json
./qc-backtest-server create-projects --count 10

# Delete projects beyond the first --keep IDs in projects.json
./qc-backtest-server delete-projects --keep 50 --yes

# Reconcile projects.json against the QC user's actual project list
./qc-backtest-server reconcile-projects
./qc-backtest-server reconcile-projects --prune-orphans
```

## Scheduling weight

Each strategy gets backtest slots proportional to its weight (default 1.0):

```sql
INSERT OR REPLACE INTO settings (category, key, value)
VALUES ('strategy', 'scheduler_weight', '3.0');
```
