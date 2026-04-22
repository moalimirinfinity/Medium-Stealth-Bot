# Promotion Policy

Policy for enabling sustained daily live scheduling.

## Gate A: Dry-Run Stability

Requirement:

- 3 consecutive daily dry-runs succeed.
- 0 failed/halted run artifacts.

Evidence:

- artifact paths + `status` output snapshot for each day.

## Gate B: Manual Live Validation

Requirement:

- 3 manual live runs on separate days.
- 0 halts.
- 0 failed run statuses.

Execution mode:

- `uv run bot start --quick-live --dry-run-first --tag programming`

## Gate C: Scheduled Soak

Requirement:

- scheduler enabled once per day for 7 consecutive days.
- 0 halted/failed scheduled runs.

Evidence:

- `.data/scheduler/` logs + run artifact summaries.

## Gate D: Sustained Schedule Approval

Requirement:

- active profile passes `bot profile-validate`.
- action caps align with approved production limits.
- kill-switch and rollback procedures are documented and rehearsed.

Outcome:

- sustained daily live schedule approved.

## Reversion Rule

If any halt/failure trend appears after promotion:

1. set `OPERATOR_KILL_SWITCH=true`
2. pause scheduler
3. return to Gate A after remediation

## Evidence Template

Use this per gate to keep promotion reviews consistent:

- Gate:
- Date (UTC):
- Command/Run mode:
- Artifact path(s):
- Status summary:
- Notes/Anomalies:
