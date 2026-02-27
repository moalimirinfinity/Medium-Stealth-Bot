# Scheduling Operations

This project is local-first. Scheduled live execution is done on the operator machine, not in cloud runners.

## Prerequisites

1. Create a production profile:
   - `cp .env.production.example .env.production`
   - fill `MEDIUM_SESSION`, `MEDIUM_CSRF`, `MEDIUM_USER_REF`.
2. Validate profile:
   - `uv run bot profile-validate --env-path .env.production`
   - this confirms baseline profile sanity; safety thresholds remain operator-defined in `.env.production`
3. Confirm manual run:
   - `uv run bot start --quick-live --dry-run-first --tag programming`

## Scheduler Runner

Use:

```bash
scripts/run_daily_live.sh --env-file .env.production --tag programming
```

Behavior:

- acquires lock to prevent overlapping runs
- skips run when `OPERATOR_KILL_SWITCH=true`
- validates production profile
- uses operator-configured safety thresholds from env
- executes dry-run preflight then live cycle
- writes logs to `.data/scheduler/`

Note:

- The scheduled runner is for growth execution. Use `uv run bot sync --live --force` separately when you need a full social-graph refresh before maintenance tasks.

## Cron (Linux/macOS)

Template: `ops/scheduling/cron.example`

Install:

1. Replace `/ABSOLUTE/PATH/TO/Medium-Stealth-Bot` in template.
2. Add line to crontab:
   - `crontab -e`
3. Verify entry:
   - `crontab -l`

Default cadence in template: daily at `09:00 UTC`.

Disable:

- remove cron entry and save.

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

1. `.data/scheduler/` receives timestamped run logs.
2. Each run produces a run artifact in `.data/runs/`.
3. `uv run bot status` reports healthy/degraded with expected mode.
