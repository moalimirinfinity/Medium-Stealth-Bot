# Promotion Policy

Policy for enabling sustained scheduled live operation.

## Gate A: Discovery and Dry-Run Stability

Requirement:

- 3 consecutive daily dry-runs succeed
- 0 failed or halted artifacts
- queue remains healthy enough for planned live windows

Evidence:

- artifact paths
- `uv run bot queue` snapshots
- `uv run bot status` output

## Gate B: Manual Live Validation

Requirement:

- 3 manual live growth runs on separate days
- 0 halts
- 0 failed live artifacts

Recommended execution:

```bash
uv run bot discover --live --source topic-recommended --source seed-followers --tag programming
uv run bot run --policy warm-engage --session
```

## Gate C: Scheduled Soak

Requirement:

- scheduler enabled once per day for 7 consecutive days
- 0 halted or failed scheduled runs
- queue does not repeatedly starve between discovery and growth windows

Evidence:

- `.data/scheduler/` logs
- queue snapshots
- run artifact summaries

## Gate D: Sustained Schedule Approval

Requirement:

- active profile passes `bot profile-validate`
- growth limits align with approved production thresholds
- discovery cadence is defined, not ad hoc
- kill-switch and rollback procedures are documented and rehearsed

Outcome:

- sustained daily live schedule approved

## Reversion Rule

If halt/failure trends appear after promotion:

1. set `OPERATOR_KILL_SWITCH=true`
2. pause scheduler
3. return to Gate A after remediation

## Evidence Template

- Gate:
- Date (UTC):
- Commands / run mode:
- Queue snapshot:
- Artifact path(s):
- Status summary:
- Notes / anomalies:
