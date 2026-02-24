# Development Plan

## Goal

Take the current scaffold to a production-ready local automation tool with explicit safety controls and measurable reliability.

## Fixed Product Contracts (Now Defined)

1. Canonical relationship model:
   - `newsletter_state`: `subscribed | unsubscribed | unknown`
   - `user_follow_state`: `following | not_following | unknown`
2. Schema migration path:
   - canonical table `relationship_state`
   - legacy `relationships.state` migrated as inferred `user_follow_state`
3. Day-boundary policy:
   - budget windows use UTC calendar day only
4. `MEDIUM_USER_REF` contract:
   - user id only (`user_id`)
   - `@username` is invalid for this field

## Phase 0 (Foundations Baseline)

### Status

Completed (with ongoing capture freshness enforcement).

### Scope

- Capture corpus and canonical manifest as source of truth.
- Auth/bootstrap path (`bot auth`) with explicit session material contracts.
- Fixed product contracts and baseline runtime invariants.

### Implemented in This Iteration

- Explicit Phase 0 definition added to this plan.
- Capture freshness + canonical manifest coherence checks automated via `scripts/check_capture_integrity.py`.
- CI gate added to fail on stale or missing canonical capture files.

### Exit Criteria

- Canonical capture pointers in `captures/manifest.json` are valid and in sync.
- Capture freshness policy is enforced in CI.
- Bootstrap/auth prerequisites are documented and executable from clean checkout.

---

## Phase 1 (Completed): Client Architecture — Fingerprint Alignment

### Status

Completed.

### Delivered

- `CLIENT_MODE=stealth|fast` implemented.
- `stealth` mode uses Playwright persistent browser profile + `APIRequestContext`.
- `fast` mode retains async `curl-cffi` for lighter development loops.
- Auth and execution reuse the same Playwright profile directory.

### Validation Criteria (Updated to Match Design)

- `bot probe --tag programming` succeeds in both modes.
- In `stealth` mode, GraphQL requests are executed through Playwright `APIRequestContext` bound to the persistent browser context profile.
- In `stealth` mode, no `curl-cffi` request path is used.

---

## Phase 2: Contract and Repository Hardening

### Status

In progress (parallel track with Phase 3 delivery).

### Scope

- Freeze operation contracts from `captures/final/implementation_ops_2026-02-24.json`.
- Normalize model layer for operation input/output typing.
- Add explicit operation classification (`read`, `mutation`, `state-verify`, `high-risk`).
- Expand repository beyond action counting to track target candidates, outcomes, and reconciliation state.
- Introduce schema migration strategy.

### Deliverables

- Strict Pydantic models for all implementation operations (no `dict[str, Any]` pass-through on core payloads).
- Operation registry metadata (risk level, required variables, expected top-level response fields).
- Repository methods for:
  - upsert user/newsletter metadata
  - relationship state updates (`newsletter_subscribe`, `user_follow`, etc.)
  - action idempotency checks
- Schema migration with version table + numbered SQL files.
- UTC day-boundary budgeting enforced consistently across repository and runtime.

### Implemented in This Iteration

- Typed response parsing for core discovery/verify/mutation paths (`typed_payloads.py`) wired into runtime logic.
- File-based numbered SQL migrations introduced under `src/medium_stealth_bot/migrations/` with `schema_migrations` tracking.
- Action idempotency keys added (`action_log.action_key`) with repository-level `INSERT OR IGNORE` semantics.
- Candidate reconciliation persistence added (`candidate_reconciliation`) and integrated into decision flow.
- New reconciliation command path (`bot reconcile`) for scheduled live follow-state verification.

### Exit Criteria

- All 13 canonical implementation operations validate successfully across `bot probe` and run-time preflight/read paths.
- Re-running the same probe/action cycle does not duplicate logical actions.
- DB schema upgrades are deterministic from a clean checkout.
- Strict operation payload typing replaces remaining generic response traversal in core action paths.

---

## Phase 3: Action Engine v1 (Dry-Run First)

### Status

In progress.

### Scope

- Implement the controlled action pipeline:
  1. Discover candidates (parse probe results for followable users with `newsletter_v3_id`).
  2. Evaluate eligibility (not already following, not recently actioned).
  3. Execute mutation (or simulate in dry-run mode).
  4. Verify follow state via `UserViewerEdge`.
  5. Persist outcome to `action_log` and `relationship_state`.
- Dry-run mode performs all read/verify calls but stubs write mutations.

### Deliverables

- `run` path that can perform bounded real actions (not just probe).
- Budget partitioning by action type (`subscribe`, `unfollow`, `clap`).
- Retry policy with per-operation backoff.
- `--dry-run` flag on `bot run` that simulates writes with full decision logging.

### Implemented in This Iteration

- Gap closure from prior review: per-action budget partition + retry/backoff now implemented and validated in runtime.
- Candidate extraction from `TopicWhoToFollowPubishersQuery` and `TopicLatestStorieQuery`.
- Optional "followers of seed user" discovery via `UserFollowers`.
- Eligibility checks: blacklist, cooldown, local/live follow-state checks.
- `--dry-run/--live` run mode with decision log output.
- Per-action daily budget partitioning (`subscribe`, `unfollow`, `clap`) on top of global daily budget.
- Retry/backoff policy with operation-specific retry tiers (query/verify/mutation).
- Live follow path: `SubscribeNewsletterV3Mutation` + `UserViewerEdge` verification + canonical state persistence.
- Follow-cycle state tracking (`follow_cycle`) for non-reciprocal cleanup windows.
- Cleanup path: non-reciprocal `UnfollowUserMutation` with verification.
- Organic interaction scaffold: optional pre-follow clap with configurable clap randomizer.

### Exit Criteria

- Dry-run produces full decision logs without side effects.
- Action execution stays within configured daily limits.
- Every mutation attempt has corresponding verification and DB record.

---

## Phase 4: Safety Guardrails and Human Timing

### Status

Completed.

### Scope

- Add guardrails to reduce account risk and noisy behavior.
- Implement the organic timing layer described in the project overview.
- Existing timing jitter in `logic.py` is an interim implementation; Phase 4 extracts timing into `timing.py` and adds risk halts.

### Implemented in This Iteration

- New centralized timing module (`timing.py`) for:
  - one-time session warm-up delay
  - bounded gaussian read delays
  - bounded non-uniform inter-action cooldowns
- New risk guard module (`safety.py`) with hard halts on:
  - configured consecutive final-operation failures
  - challenge signatures from response text/error payloads
  - auth/session-expiry status or token signatures
- Runtime wiring updates:
  - risk guard integrated into `_execute_with_retry`
  - action cooldown enforced before each live clap/subscribe/unfollow action
  - probe task cancellation on halt/error to prevent trailing requests
- CLI halt surfacing:
  - probe/run commands now show structured safety-halt output and exit code `2`.

### Deliverables

- `timing.py` module with:
  - Randomized read pauses (30–90s with gaussian jitter).
  - Inter-action cooldowns (non-uniform).
  - Session warm-up delays.
- Hard stop conditions:
  - Error-rate threshold (e.g., 3 consecutive failures → halt).
  - Cloudflare challenge detection (response-body heuristics).
  - Auth/session expiry detection.
- Configurable cooldown windows via settings.

### Exit Criteria

- Bot halts automatically on configured risk thresholds.
- Action timing is non-deterministic and falls within configured bounds.
- No two consecutive actions happen faster than a configurable minimum gap.

---

## Phase 5: Observability and Diagnostics

### Status

Completed.

### Scope

- Improve production debugging for cron-driven runs.

### Implemented in This Iteration

- Run correlation context now binds per command execution with `run_id` contextvars.
- Structured operation/decision events include:
  - `operation`
  - `target_id`
  - `decision`
  - `result`
- `bot run` now persists a machine-readable artifact under `.data/runs/` (plus `.data/runs/latest.json`) that includes:
  - top-level run metadata and health status
  - action counts
  - decision result counts
  - decision reason counts
  - optional error block for halted/failed runs
- New diagnostics command `bot status` renders latest run health + summary directly from run artifacts.

### Deliverables

- Structured event taxonomy (`run_id`, `operation`, `target_id`, `decision`, `result`).
- Run summary artifact written to `.data/runs/<timestamp>.json`.
- CLI diagnostics command (`bot status`) for last-run health.

### Exit Criteria

- A failed run can be diagnosed from logs/artifact without code changes.
- End-of-run summary includes counts by action/result/reason.

---

## Phase 6: Tests and CI Baseline

### Scope

- Introduce automated quality gates.

### Deliverables

- Unit tests for settings, operation builders, repository logic.
- Integration tests with stubbed GraphQL responses.
- CI workflow: lint + tests + type checks.

### Exit Criteria

- CI passes on clean branch.
- Critical paths (`auth`, `probe`, `run` dry-run) covered by automated tests.

---

## Phase 7: Runbook and Release

### Scope

- Finalize operational guidance for safe daily usage.

### Deliverables

- Operator runbook:
  - First-time setup.
  - Routine run schedule.
  - Auth refresh procedure.
  - Incident rollback steps.
- Release checklist and versioning conventions.

### Exit Criteria

- New operator can run safely from docs only.
- Recovery from expired/broken session is documented and validated.

---

## Active Priority Order (Renumbered)

1. **Phase 2** — Harden contracts and data layer.
2. **Phase 3** — Action engine with dry-run (first real capability).
3. **Phase 4** — Safety and timing (production readiness).
4. **Phase 5** — Observability.
5. **Phase 6** — Tests and CI.
6. **Phase 7** — Runbook and release.

---

## Go-Live Checklist (Strict)

### Decision Rule

`READY_FOR_DEPLOY=true` only when every Hard Gate below is `PASS` on the release commit.
If any Hard Gate is `FAIL`, release is blocked.

### Gate 1: Contracts and Data Layer (Phase 2) — Hard

Pass Criteria:
- `uv run bot contracts --tag programming --no-execute-reads` exits `0`.
- Contract registry/code parity is exact (`13` registry operations == implemented operations).
- DB migration is deterministic and idempotent from clean state.
- Core action paths do not rely on untyped payload traversal for contract-critical fields.

Evidence Commands:
```bash
uv run bot contracts --tag programming --no-execute-reads
uv run python - <<'PY'
from pathlib import Path
import sqlite3, tempfile
from medium_stealth_bot.database import Database
with tempfile.TemporaryDirectory() as td:
    p = Path(td) / "gate1.db"
    db = Database(p)
    db.initialize()
    db.initialize()
    con = sqlite3.connect(p)
    print("user_version", con.execute("PRAGMA user_version").fetchone()[0])
PY
```

### Gate 2: Action Engine Integrity (Phase 3) — Hard

Pass Criteria:
- `bot run --dry-run` executes end-to-end without write mutations.
- Daily budget partitioning and retry behavior are applied and reported.
- Every live mutation path has explicit verify step + persistence path.

Evidence Commands:
```bash
uv run bot run --tag programming --dry-run
```

### Gate 3: Safety Guardrails (Phase 4) — Hard

Pass Criteria:
- Timing/rate logic is centralized (target module: `timing.py`).
- Hard stop conditions are implemented and exercised:
  - repeated failure threshold halt
  - challenge-signal halt
  - session-expiry halt
- Minimum action gap is enforced by configuration.

Evidence:
- Code references for halt logic and timing integration.
- Automated tests or deterministic reproductions for each halt condition.

### Gate 4: Observability and Diagnostics (Phase 5) — Hard

Pass Criteria:
- Each run emits a machine-readable run artifact under `.data/runs/`.
- Structured logs include correlation keys (`run_id`, `operation`, `target_id`, `decision`, `result`).
- `bot status` (or equivalent diagnostics command) reports last-run health without code edits.

Evidence:
- Sample artifact from a successful run and a failed run.
- CLI screenshot/output for diagnostics command.

### Gate 5: Tests and CI Baseline (Phase 6) — Hard

Pass Criteria:
- CI passes on release branch for lint, tests, type checks, and contract gate.
- Unit coverage includes settings, operation builders, repository/day-budget logic.
- Integration coverage includes probe and run dry-run with stubbed responses.

Evidence:
- Latest CI run URL and commit SHA.
- Test summary report attached to release candidate.

### Gate 6: Runbook and Operational Readiness (Phase 7) — Hard

Pass Criteria:
- Operator runbook covers setup, routine schedule, auth refresh, and incident rollback.
- Recovery steps are validated by a fresh operator run on a clean environment.
- Release checklist and rollback checklist are completed and signed.

Evidence:
- Runbook document path.
- Signed release checklist record (date, owner, commit).

### Optional Pre-Release Confidence Check (Recommended)

Use live read-only contract checks right before enabling live mutations:
```bash
uv run bot contracts --tag programming --execute-reads
```

### Release Verdict Template

- Date:
- Commit SHA:
- Gate 1 (Phase 2): `PASS|FAIL`
- Gate 2 (Phase 3): `PASS|FAIL`
- Gate 3 (Phase 4): `PASS|FAIL`
- Gate 4 (Phase 5): `PASS|FAIL`
- Gate 5 (Phase 6): `PASS|FAIL`
- Gate 6 (Phase 7): `PASS|FAIL`
- Final Verdict: `READY_FOR_DEPLOY=true|false`
