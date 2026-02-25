# Rollback Procedure

Use this when a live rollout/regression must be reverted quickly.

## Immediate Stop

1. Set `OPERATOR_KILL_SWITCH=true` in active profile.
2. Stop scheduler:
   - cron: remove job line
   - launchd: `launchctl unload ...`
3. Confirm no new live runs are executed.

## Rollback to Previous Tag

1. Identify stable previous tag:
   - `git tag --sort=-v:refname`
2. Checkout previous tag on deployment checkout:
   - `git checkout <previous-tag>`
3. Sync dependencies:
   - `uv sync --group dev`

## Re-validate Before Resume

1. `uv run bot profile-validate --env-path .env.production`
   - baseline profile check
2. `uv run bot contracts --tag programming --no-execute-reads`
3. `uv run bot start --quick-live --dry-run-first --tag programming`

## Controlled Resume

1. Set `OPERATOR_KILL_SWITCH=false`.
2. Re-enable scheduler.
3. Monitor first resumed run via:
   - `.data/scheduler/*.log`
   - `uv run bot status`

## Incident Record Template

- Date/time (UTC):
- Triggering change/tag:
- Halt reason:
- Rollback target tag:
- Verification evidence:
- Follow-up action items:
