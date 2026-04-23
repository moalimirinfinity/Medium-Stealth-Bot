# Live Operations Runbook

## Menu Quick Map (`uv run bot start`)

Current interactive sections:

- `1`: discovery
- `2`: growth
- `3`: unfollow
- `4`: maintenance
- `5`: diagnostics
- `6`: observability
- `7`: settings/auth
- `8`: exit

## Preflight Checklist

1. `uv run bot profile-validate --env-path .env.production` passes.
2. `uv run bot contracts --tag programming --no-execute-reads` passes.
3. Optional live-read contract check passes if newsletter inputs are configured.
4. `OPERATOR_KILL_SWITCH=false` in the active profile.
5. No stale scheduler lock exists under `.data/scheduler/`.
6. If local graph cache is stale, refresh it before live operations:
   - `uv run bot sync --live --force`

## Manual Operator SOP

### A. Fill the queue

Run discovery first when queue ready is low or empty.

```bash
uv run bot discover --live --source topic-recommended --source seed-followers --tag programming
uv run bot queue
```

### B. Execute growth

Use queue-driven execution after discovery.

```bash
uv run bot run --policy warm-engage --session
```

Use dry-run when checking the next pass without mutations.

```bash
uv run bot run --policy warm-engage --dry-run --single-cycle
```

### C. Inspect output

```bash
uv run bot status
uv run bot observe validate-artifact
```

## Quick-Live SOP

For the guided operator path:

```bash
uv run bot start --quick-live --dry-run-first --tag programming
```

Use this when you want the menu-oriented live flow rather than explicit command-by-command control.

## Daily Operator Loop

1. Validate profile and contracts.
2. Check queue status:
   - `uv run bot queue`
3. Run discovery if ready queue is low.
4. Run live growth.
5. Review `status` and latest artifact validation.
6. Run maintenance only when needed.

## Discovery SOP

### General refill

```bash
uv run bot discover --live --source topic-recommended --source seed-followers --tag programming
```

### Target-user follower harvest

```bash
uv run bot growth followers --target-user @username --live
```

### Dry-run preview

```bash
uv run bot discover --dry-run --source responders --tag programming
```

Expected result:

- ready queue grows when useful candidates are found
- no follow/clap/comment mutations occur in discovery

## Growth SOP

### Single pass

```bash
uv run bot run --policy follow-only --single-cycle
```

### Live session

```bash
uv run bot run --policy warm-engage --session
```

### Preflight

```bash
uv run bot growth preflight --policy warm-engage
```

Expected result:

- growth drains execution-ready queue only
- follow state is re-checked right before mutation
- if the ready queue is empty, stop and run discovery first

## Maintenance SOP

### Full social graph refresh

```bash
uv run bot sync --live --force
```

### Reconcile follow states

```bash
uv run bot reconcile --live --limit 200 --page-size 50
uv run bot reconcile --dry-run --limit 200 --page-size 50
```

### Cleanup-only unfollow

```bash
uv run bot cleanup --dry-run --limit 50
uv run bot cleanup --live --limit 50
```

### DB hygiene

```bash
uv run bot maintenance db-hygiene --dry-run
uv run bot maintenance db-hygiene --live --vacuum
```

## Scheduled Live SOP

1. Install the scheduler from [docs/SCHEDULING.md](/Users/moalimir/Project%20World/Medium-Stealth-Bot/docs/SCHEDULING.md).
2. Confirm logs under `.data/scheduler/`.
3. Check `uv run bot status` after scheduled runs.
4. Schedule separate discovery runs or keep the queue sufficiently stocked before scheduled growth windows.

## Halt Reason Triage

### `challenge`

Likely upstream anti-bot challenge or protection event.

Actions:

1. set `OPERATOR_KILL_SWITCH=true`
2. refresh auth with `uv run bot auth`
3. rerun dry checks before resuming live

### `session_expiry`

Session or CSRF material is no longer valid.

Actions:

1. set `OPERATOR_KILL_SWITCH=true`
2. run `uv run bot auth` or `uv run bot auth-import`
3. rerun profile validation and dry checks

### `consecutive_failures`

Repeated final-attempt failures exceeded the halt threshold.

Actions:

1. set `OPERATOR_KILL_SWITCH=true`
2. inspect the latest artifact and logs
3. validate contracts and run `probe`
4. only resume after the root cause is understood

## Kill-Switch Procedure

1. Set `OPERATOR_KILL_SWITCH=true` in the active env file.
2. Stop the scheduler.
3. Confirm no new live runs are executed.

## Recovery and Resume

1. Resolve auth, challenge, contract, or config issues.
2. Refill the queue if needed:
   - `uv run bot discover --live ...`
3. Set `OPERATOR_KILL_SWITCH=false`.
4. Resume with a dry-run growth pass first.
