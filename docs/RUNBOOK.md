# Live Operations Runbook

## Menu Quick Map (`uv run bot start`)

Use these maintenance options from the interactive menu:

- `5`: cleanup-only unfollow (live)
- `6`: cleanup-only unfollow (dry-run)
- `7`: reconcile follow states (live)
- `8`: reconcile follow states (dry-run)
- `9`: sync social graph cache

## Preflight Checklist

1. `uv run bot profile-validate --env-path .env.production` passes.
   - baseline check only; confirm your chosen safety thresholds in `.env.production`.
2. `uv run bot contracts --tag programming --no-execute-reads` passes.
3. Optional live read check passes when newsletter inputs are configured.
4. `OPERATOR_KILL_SWITCH=false` in active profile.
5. No active lock from previous scheduled run (`.data/scheduler/run_daily_live.lock` absent).
6. If running cleanup-heavy maintenance, refresh cache first:
   - `uv run bot sync --live --force`

## Manual Live Run SOP

1. Execute:
   - `uv run bot start --quick-live --dry-run-first --tag programming`
2. Inspect output tables and run artifact path.
3. Execute:
   - `uv run bot status`
4. Validate artifact schema:
   - `uv run bot artifacts validate`

## Scheduled Live SOP

1. Install scheduler from `docs/SCHEDULING.md`.
2. Confirm daily logs under `.data/scheduler/`.
3. Review latest run daily via:
   - `uv run bot status`

## Daily Operator Loop

1. Preflight validate profile + contracts.
2. Run `start --quick-live --dry-run-first` or scheduled runner.
3. Check `bot status` + artifact validation.
4. Run maintenance (`sync/reconcile/cleanup`) when needed.

## Maintenance SOP

### Full Social Graph Refresh

1. Execute:
   - `uv run bot sync --live --force`
2. Confirm snapshot counts from command output/artifact (`followers_count`, `following_count`).
3. Verify latest state:
   - `uv run bot status`

### Reconcile Follow States

1. Execute live:
   - `uv run bot reconcile --live --limit 200 --page-size 50`
2. Use dry-run when validating only:
   - `uv run bot reconcile --dry-run --limit 200 --page-size 50`
3. Confirm scanned/updated counts in output and artifact summary.

### Cleanup-Only Unfollow

1. Execute dry-run first:
   - `uv run bot cleanup --dry-run --limit 50`
2. Execute live run:
   - `uv run bot cleanup --live --limit 50`
3. Expected behavior:
   - only users present in `own_following_cache` are unfollow candidates
   - high-follower users are retained by whitelist threshold
   - live unfollows run with short randomized gaps in an effective `1-4s` window

## Halt Reason Triage

### `challenge`

Likely anti-bot challenge or upstream protection.

Actions:

1. set `OPERATOR_KILL_SWITCH=true`
2. refresh auth (`uv run bot auth`)
3. re-run dry checks before resuming live

### `session_expiry`

Session cookies/CSRF no longer valid.

Actions:

1. set `OPERATOR_KILL_SWITCH=true`
2. run `uv run bot auth`
3. validate profile and run dry preflight

### `consecutive_failures`

Multiple final-attempt failures reached threshold.

Actions:

1. set `OPERATOR_KILL_SWITCH=true`
2. inspect latest run artifact + logs
3. validate contracts and probe
4. only resume after root cause is addressed

## Kill-Switch Procedure

1. In active env profile, set `OPERATOR_KILL_SWITCH=true`.
2. Stop current scheduler service (cron/launchd unload).
3. Confirm next scheduler trigger performs no-op.

## Recovery and Resume

1. Resolve cause (auth, challenge, API change, config drift).
2. Set `OPERATOR_KILL_SWITCH=false`.
3. Run:
   - `uv run bot start --quick-live --dry-run-first --tag programming`
4. Re-enable scheduler.
