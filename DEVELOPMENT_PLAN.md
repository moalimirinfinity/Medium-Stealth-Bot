# Development Plan

## Goal

Take the current scaffold to a production-ready local automation tool with explicit safety controls and measurable reliability.

## Phase 1: Client Architecture — Fix the Fingerprint Split

### Problem

Authentication runs through **Playwright Chromium** (real Chrome TLS fingerprint), but all subsequent GraphQL execution goes through **`curl-cffi` with `impersonate="chrome120"`** (synthetic Chrome TLS fingerprint). These produce similar but non-identical JA3/JA4 signatures. Cloudflare can correlate that a session born from fingerprint X is now making requests from fingerprint Y — a classic detection vector.

### Scope

- Introduce a **Playwright API request context** client mode that reuses the persistent browser profile from `auth`.
- Requests inherit the browser's exact TLS stack, cookies, and headers — zero fingerprint mismatch.
- Retain `curl-cffi` as a `"fast"` mode for local development and testing.
- Client mode is controlled by a single setting: `CLIENT_MODE=stealth|fast`.

### Deliverables

- `client.py` refactored: abstract `BaseClient` protocol, `CurlCffiClient` (existing), `PlaywrightClient` (new).
- `settings.py` gains `client_mode` field with `stealth` default.
- `main.py` wires the correct client based on settings.
- Auth and execution share the same Playwright persistent profile directory.

### Exit Criteria

- `bot probe --tag programming` works identically in both modes.
- In `stealth` mode, auth fingerprint and execution fingerprint are the same (verified by checking that no separate browser process or TLS library is involved in the request path).

---

## Phase 2: Contract and Repository Hardening

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

### Exit Criteria

- All 13 implementation operations validate successfully in `bot probe`.
- Re-running the same probe/action cycle does not duplicate logical actions.
- DB schema upgrades are deterministic from a clean checkout.

---

## Phase 3: Action Engine v1 (Dry-Run First)

### Scope

- Implement the controlled action pipeline:
  1. Discover candidates (parse probe results for followable users with `newsletter_v3_id`).
  2. Evaluate eligibility (not already following, not recently actioned).
  3. Execute mutation (or simulate in dry-run mode).
  4. Verify follow state via `UserViewerEdge`.
  5. Persist outcome to `action_log` and `relationships`.
- Dry-run mode performs all read/verify calls but stubs write mutations.

### Deliverables

- `run` path that can perform bounded real actions (not just probe).
- Budget partitioning by action type (`subscribe`, `unsubscribe`, `unfollow`, `clap`).
- Retry policy with per-operation backoff.
- `--dry-run` flag on `bot run` that simulates writes with full decision logging.

### Exit Criteria

- Dry-run produces full decision logs without side effects.
- Action execution stays within configured daily limits.
- Every mutation attempt has corresponding verification and DB record.

---

## Phase 4: Safety Guardrails and Human Timing

### Scope

- Add guardrails to reduce account risk and noisy behavior.
- Implement the organic timing layer described in the project overview.

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

### Scope

- Improve production debugging for cron-driven runs.

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

## Immediate Priority Order

1. **Phase 1** — Fix the fingerprint split (foundational, blocks everything).
2. **Phase 2** — Harden contracts and data layer.
3. **Phase 3** — Action engine with dry-run (first real capability).
4. **Phase 4** — Safety and timing (production readiness).
5. **Phase 5** — Observability.
6. **Phase 6** — Tests and CI.
7. **Phase 7** — Runbook and release.
