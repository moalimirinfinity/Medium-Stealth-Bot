# Development Plan

## Objective

Evolve this repository into a production-operable, local-first Medium automation tool with:

- explicit product contracts
- safety-first runtime behavior
- deterministic migrations and local state integrity
- contract-driven operation stability
- diagnostics good enough for unattended runs

## Product Contracts (Fixed)

1. Canonical relationship model:
   - `newsletter_state`: `subscribed | unsubscribed | unknown`
   - `user_follow_state`: `following | not_following | unknown`
2. Day-boundary policy:
   - all action budgets are computed on UTC day boundaries only
3. `MEDIUM_USER_REF`:
   - must be a Medium `user_id`
   - `@username` is invalid for this field
4. Verification rule:
   - user follow-state truth is derived from `UserViewerEdge`
   - newsletter state and user-follow state are intentionally independent

## Phase Status Matrix

| Phase | Name | Status |
| --- | --- | --- |
| 0 | Foundations Baseline | Completed |
| 1 | Client Architecture | Completed |
| 2 | Contract + Repository Hardening | Completed |
| 3 | Action Engine v1 | Completed |
| 4 | Safety Guardrails + Timing | Completed |
| 5 | Observability + Diagnostics | Completed |
| 6 | Tests + CI Baseline | Completed |
| 7 | Deployment Hardening | In progress |

---

## Phase 0: Foundations Baseline

### Delivered

- Canonical capture inventory and manifest-based references.
- Auth/bootstrap flow via `bot auth`.
- Fixed product contracts documented and enforced in runtime config.
- Capture integrity script (`scripts/check_capture_integrity.py`) + CI gate.

### Exit Criteria

- Canonical capture pointers resolve and stay fresh.
- New checkout can bootstrap with documented auth flow.

---

## Phase 1: Client Architecture

### Delivered

- Dual client execution modes:
  - `CLIENT_MODE=stealth`: Playwright persistent profile + `APIRequestContext`
  - `CLIENT_MODE=fast`: async `curl-cffi`
- Stealth mode keeps auth and execution in the same browser profile context.
- Mode-level smoke coverage exists in tests.

### Exit Criteria

- Probe and dry-run paths execute in both modes.

---

## Phase 2: Contract + Repository Hardening

### Delivered

- Contract registry based on `captures/final/implementation_ops_2026-02-24.json`.
- Request and response contract validation (including expected response paths).
- Typed payload parsing for core read/verify/mutation flows (`typed_payloads.py`).
- File-based SQL migrations with checksum history:
  - `migrations/<version>_<name>.sql`
  - `schema_migrations(version, name, checksum, applied_at)`
- Candidate reconciliation persistence (`candidate_reconciliation`).
- Idempotency keys (`action_log.action_key` + unique index + `INSERT OR IGNORE` writes).

### Exit Criteria

- Contract parity checks pass.
- Live read checks pass in strict response-field mode when required inputs are provided.
- Migrations are deterministic on clean and upgraded databases.

---

## Phase 3: Action Engine v1

### Delivered

- End-to-end run pipeline:
  1. discovery
  2. scoring
  3. eligibility filtering
  4. follow/subscribe attempt
  5. post-mutation follow verification
  6. state persistence and decision logging
- Candidate sources:
  - topic latest stories
  - topic who-to-follow
  - who-to-follow module
  - optional seed followers (+ optional second hop)
- Optional pre-follow clap path with verification.
- Follow-cycle tracking and non-reciprocal cleanup pipeline.
- Dedicated reconciliation command:
  - `bot reconcile --limit --page-size [--dry-run]`

### Exit Criteria

- Dry-run has no side-effect mutations and still emits full decision traces.
- Live mutation paths persist verification outcomes and relationship state.
- Duplicate logical actions are prevented by idempotency keys.

---

## Phase 4: Safety Guardrails + Timing

### Delivered

- Timing controller (`timing.py`) for:
  - session warm-up delay
  - read delays
  - inter-action gaps
- Risk guard (`safety.py`) hard-halts on:
  - challenge signatures/status
  - session expiry/auth failure signatures
  - consecutive final-attempt failures
  - operator kill switch (`OPERATOR_KILL_SWITCH=true`)

### Exit Criteria

- Risk conditions halt runs predictably.
- Action pacing stays within configured ranges and minimum gaps.

---

## Phase 5: Observability + Diagnostics

### Delivered

- Structured event fields (`run_id`, `operation`, `target_id`, `decision`, `result`).
- Machine-readable run artifacts written to `.data/runs/`.
- Artifact schema validation (`bot artifacts validate`).
- Human diagnostics summary (`bot status`).
- Redaction layer for sensitive material in logs/artifacts.

### Exit Criteria

- Failed/halted runs can be diagnosed from artifact + logs without code edits.

---

## Phase 6: Tests + CI Baseline

### Delivered

- Local test suite for:
  - client modes
  - migrations
  - repository idempotency
  - safety/timing behavior
  - dry-run fixture replay
  - artifact schema and redaction
- CI quality workflow:
  - compile
  - capture integrity
  - response-contract path checks
  - pytest
  - contract registry checks
  - optional live read checks when secrets/vars are available

### Exit Criteria

- CI green on clean branch.
- Core command paths have automated coverage.

---

## Phase 7: Deployment Hardening (Current Focus)

### Delivered so far

- Guided CLI onboarding and execution commands:
  - `bot setup` interactive profile wizard
  - `bot start` live-default guided execution flow (optional `--dry-run-first` preflight)

### Remaining

1. Release workflow:
   - add explicit deploy/schedule workflow separate from quality checks.
2. Production profile:
   - lock prod defaults for action budgets, delays, and guardrails.
3. Runbook:
   - document live incident response and rollback procedures.
4. Promotion process:
   - define rollout/soak criteria for sustained live schedule (with optional dry-run preflight gates).

### Exit Criteria

- One-command/one-workflow release path exists.
- Production runbook is documented and tested.
- Scheduled live automation can be paused instantly via kill switch and resumed safely.
