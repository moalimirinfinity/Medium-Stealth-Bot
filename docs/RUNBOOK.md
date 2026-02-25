# Live Operations Runbook

## Preflight Checklist

1. `uv run bot profile-validate --env-path .env.production` passes.
2. `uv run bot contracts --tag programming --no-execute-reads` passes.
3. Optional live read check passes when newsletter inputs are configured.
4. `OPERATOR_KILL_SWITCH=false` in active profile.
5. No active lock from previous scheduled run (`.data/scheduler/run_daily_live.lock` absent).

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
