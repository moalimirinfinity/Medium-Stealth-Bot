# Scheduling Operations

This project is local-first. Scheduled execution runs on the operator machine, not in hosted runners.

## What Should Be Scheduled

The project now has two separate operational loops:

- `Discovery`: fills the queue with execution-ready candidates
- `Growth`: drains the queue with live follow/engagement actions

The bundled runner in `scripts/run_daily_live.sh` is for the guided live workflow driven by `bot start --quick-live --dry-run-first`.
In the current project model, treat scheduled growth and scheduled discovery as separate concerns.
Discovery is responsible for candidate screening before rows enter the queue. Scheduled growth assumes the ready queue has already been evaluated and only performs action-time guards such as budgets, pacing, post-context preparation, and the final live follow-state check. Live discovery targets 100 eligible candidates per run by default, while the growth candidate queue is capped at 700 rows. Score explanations are persisted with queued candidates and follow-cycle rows so conservative learning can adjust future ranking from completed outcomes. Live discovery/growth runs purge non-actionable legacy queue rows before selecting work.

## Prerequisites

1. Create a production profile:
   - `cp .env.production.example .env.production`
2. Fill auth/session values:
   - `MEDIUM_SESSION` or `MEDIUM_SESSION_*`
   - `MEDIUM_CSRF`
   - `MEDIUM_USER_REF`
3. Validate the profile:
   - `uv run bot profile-validate --env-path .env.production`
4. Confirm a manual run:
   - `uv run bot start --quick-live --dry-run-first --tag programming`

## Bundled Scheduler Runner

Use:

```bash
scripts/run_daily_live.sh --env-file .env.production --tag programming
```

Behavior:

- acquires a local lock
- skips when `OPERATOR_KILL_SWITCH=true`
- validates the production profile
- runs `bot start --quick-live --dry-run-first`
- writes logs to `.data/scheduler/`

Important:

- This runner is intended for the guided live workflow.
- Keep the growth queue stocked before relying on scheduled growth windows.
- Run discovery separately on a cadence that matches queue depletion.

## Suggested Scheduling Strategy

### Simple setup

1. Run discovery manually or on a separate scheduled cadence.
2. Use the bundled runner once per day for live execution.
3. Check queue counts regularly with:
   - `uv run bot queue`

### More controlled setup

Use two scheduled jobs:

1. `discover` job
   - fills the queue
2. `run` or `start --quick-live` job
   - drains the queue

This separation matches the current project architecture better than a single combined automation assumption.

## Cron (Linux/macOS)

Template: `ops/scheduling/cron.example`

Install:

1. Replace `/ABSOLUTE/PATH/TO/Medium-Stealth-Bot` in the template.
2. Add the line to your crontab with `crontab -e`.
3. Verify with `crontab -l`.

Default template cadence: daily at `09:00 UTC`.

Disable:

- remove the cron entry

## launchd (macOS)

Template: `ops/scheduling/com.mediumstealthbot.daily.plist`

Install:

1. Replace `/ABSOLUTE/PATH/TO/Medium-Stealth-Bot` placeholders.
2. Copy into `~/Library/LaunchAgents/`.
3. Load:
   - `launchctl load ~/Library/LaunchAgents/com.mediumstealthbot.daily.plist`
4. Verify:
   - `launchctl list | rg mediumstealthbot`

Disable:

- `launchctl unload ~/Library/LaunchAgents/com.mediumstealthbot.daily.plist`

## Verification Checklist

1. `.data/scheduler/` receives timestamped logs.
2. Runs emit artifacts under `.data/runs/`.
3. `uv run bot status` reports the latest run outcome.
4. `uv run bot queue` shows whether discovery cadence is keeping up with growth.

## Troubleshooting

### Scheduler does not run

1. Confirm `.data/scheduler/run_daily_live.lock` is not stale.
2. Confirm `OPERATOR_KILL_SWITCH=false`.

### Scheduled growth runs but does nothing

1. Check `uv run bot queue`.
2. If ready queue is empty, run discovery first:
   - `uv run bot discover --live --source topic-recommended --source seed-followers --tag programming`

### Local graph state is stale

Run:

```bash
uv run bot sync --live --force
```

### Preflight fails

Run:

```bash
uv run bot profile-validate --env-path .env.production
uv run bot contracts --tag programming --no-execute-reads
```
