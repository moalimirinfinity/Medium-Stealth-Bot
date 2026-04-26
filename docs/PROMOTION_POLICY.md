# Promotion Policy

Policy for enabling sustained scheduled live operation.

## Gate A: Discovery and Dry-Run Stability

Requirement:

- 3 consecutive daily dry-runs succeed
- 0 failed or halted artifacts
- queue remains healthy enough for planned live windows
- discovery is the only candidate eligibility gate before queue insertion
- discovery score explanations are persisted for queued candidates and later follow-cycle outcomes
- discovery can consistently add up to the configured per-run eligible target, default 100, without exceeding the configured queue cap, default 700
- non-actionable queue rows are absent or cleaned by the next live discovery/growth run

Evidence:

- artifact paths
- `uv run bot queue` snapshots
- `uv run bot status` output

Recommended dry-run command:

```bash
uv run bot growth cycle --policy warm-engage --dry-run
```

Do not use `growth preflight` for dry-run-only evidence; that grouped alias currently runs a dry-run pass and then continues into a live session.

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
- growth schedule is paired with a queue-refill process that keeps execution-ready candidates available
- verified follows remove candidates from the growth queue
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
