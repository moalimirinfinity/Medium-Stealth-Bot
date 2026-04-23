# Rollback Procedure

Use this when a live rollout or operational regression must be reverted quickly.

## Immediate Stop

1. Set `OPERATOR_KILL_SWITCH=true` in the active profile.
2. Stop the scheduler:
   - cron: remove or disable the job
   - launchd: `launchctl unload ...`
3. Confirm no new live growth runs are executed.

## Roll Back to Previous Tag

1. Identify the stable previous tag:
   - `git tag --sort=-v:refname`
2. Check out the previous tag:
   - `git checkout <previous-tag>`
3. Sync dependencies:
   - `uv sync --group dev`

Quick sequence:

```bash
git tag --sort=-v:refname
git checkout <previous-tag>
uv sync --group dev
```

## Re-Validate Before Resume

1. Validate the production profile:
   - `uv run bot profile-validate --env-path .env.production`
2. Validate contracts:
   - `uv run bot contracts --tag programming --no-execute-reads`
3. Refill the queue if necessary:
   - `uv run bot discover --live --source topic-recommended --source seed-followers --tag programming`
4. Run a dry growth pass:
   - `uv run bot run --policy warm-engage --dry-run --single-cycle`

## Controlled Resume

1. Set `OPERATOR_KILL_SWITCH=false`.
2. Re-enable the scheduler.
3. Watch the first resumed run with:
   - `.data/scheduler/*.log`
   - `uv run bot status`
   - `uv run bot queue`

## Incident Record Template

- Date/time (UTC):
- Triggering change/tag:
- Affected pipeline:
- Halt reason:
- Rollback target tag:
- Verification evidence:
- Follow-up action items:
