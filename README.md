# Medium Stealth Bot

Local-first Medium automation focused on safety guardrails, capture-driven contracts, and auditable local state.

## What It Does

- Runs Medium discovery/follow/cleanup loops with strict daily budgets.
- Verifies follow state using canonical read checks.
- Persists decisions and outcomes into local SQLite state.
- Emits machine-readable run artifacts for diagnostics.

## Stack

- `uv` + `pyproject.toml`
- Python 3.12+
- `Typer` + `Rich`
- `Pydantic v2` + `pydantic-settings`
- `structlog`
- `SQLite`
- `Playwright` (stealth mode) + `curl-cffi` (fast mode)

## Execution Modes

1. `CLIENT_MODE=stealth` (default)
   - Playwright persistent profile + `APIRequestContext`
2. `CLIENT_MODE=fast`
   - async `curl-cffi` for lighter local loops

## Quickstart

```bash
uv sync --group dev
uv run playwright install chromium
cp .env.example .env
uv run bot --help
```

Authenticate once:

```bash
uv run bot auth
```

This writes:

- `MEDIUM_SESSION`
- `MEDIUM_CSRF`
- `MEDIUM_USER_REF`

Or use guided setup:

```bash
uv run bot setup
```

This wizard can:

- capture auth if missing
- set common runtime defaults (mode, budgets, discovery depth, seed users)
- save everything into `.env`

## Core Commands

```bash
uv run bot setup
uv run bot start
uv run bot start --dry-run-first
uv run bot probe --tag programming
uv run bot contracts --tag programming
uv run bot contracts --tag programming --execute-reads \
  --newsletter-slug "$CONTRACT_REGISTRY_LIVE_NEWSLETTER_SLUG" \
  --newsletter-username "$CONTRACT_REGISTRY_LIVE_NEWSLETTER_USERNAME"
uv run bot run --tag programming
uv run bot run --tag programming --dry-run --seed-user @some_creator
uv run bot reconcile --limit 200 --page-size 50
uv run bot reconcile --dry-run --limit 200 --page-size 50
uv run bot artifacts validate
uv run bot status
```

### Command Notes

- `setup`: interactive wizard for auth + common `.env` defaults.
- `start`: guided entrypoint that executes live by default, with optional dry-run preflight.
- `probe`: parallel read-only GraphQL health checks.
- `contracts`: implementation-registry parity and optional live read checks.
- `run`: full daily cycle (discovery, scoring, follow pipeline, cleanup), live by default.
- `reconcile`: follow-state reconciliation over persisted candidates/follow-cycle rows, live by default.
- `artifacts validate`: schema/version validation for run artifacts.
- `status`: diagnostics summary from latest artifact.

## Product Rules

1. Budget windows are UTC day boundaries only.
2. `MEDIUM_USER_REF` must be a Medium `user_id` (not `@username`).
3. Newsletter state and user-follow state are tracked separately.
4. Canonical follow-state truth is `UserViewerEdge.isFollowing`.
5. `PublishPostThreadedResponse` is contract-covered but excluded from default daily execution.

## Key Configuration

See `.env.example` for the full set. Important variables:

- `CLIENT_MODE`
- `DAY_BOUNDARY_POLICY`
- `LOG_LEVEL`
- `LOG_FORMAT` (`pretty` default, `json` for machine log parsing)
- `MAX_ACTIONS_PER_DAY`
- `MAX_SUBSCRIBE_ACTIONS_PER_DAY`
- `MAX_UNFOLLOW_ACTIONS_PER_DAY`
- `MAX_CLAP_ACTIONS_PER_DAY`
- `MAX_FOLLOW_ACTIONS_PER_RUN`
- `FOLLOW_CANDIDATE_LIMIT`
- `FOLLOW_COOLDOWN_HOURS`
- `UNFOLLOW_NONRECIPROCAL_AFTER_DAYS`
- `RECONCILE_SCAN_LIMIT`
- `RECONCILE_PAGE_SIZE`
- `SCORE_WEIGHT_RATIO`
- `SCORE_WEIGHT_KEYWORD`
- `SCORE_WEIGHT_SOURCE`
- `SCORE_WEIGHT_NEWSLETTER`
- `CONTRACT_REGISTRY_VALIDATE_RESPONSE_FIELDS`
- `CONTRACT_REGISTRY_LIVE_NEWSLETTER_SLUG`
- `CONTRACT_REGISTRY_LIVE_NEWSLETTER_USERNAME`
- `RISK_HALT_CONSECUTIVE_FAILURES`
- `ENABLE_CHALLENGE_HALT`
- `ENABLE_SESSION_EXPIRY_HALT`
- `OPERATOR_KILL_SWITCH`

### Logging Modes

- Default (`LOG_FORMAT=pretty`) prints concise, human-friendly event lines and rich summary tables.
- `LOG_FORMAT=json` restores structured JSON lines for ingestion and machine parsing.
- Quick overrides:
  - `LOG_FORMAT=pretty uv run bot run --tag programming --dry-run`
  - `LOG_FORMAT=json uv run bot run --tag programming --dry-run`

## Safety Model

Runtime halts on:

- challenge signatures/statuses
- auth/session-expiry signatures
- consecutive failure threshold
- operator kill switch

Timing behavior includes session warm-up, read delays, and inter-action gaps.

## Local State and Artifacts

- DB path: `.data/medium-stealth-bot.db`
- Artifacts: `.data/runs/` + `.data/runs/latest.json`
- Migrations: `src/medium_stealth_bot/migrations/`
- Migration history: `schema_migrations`

## Quality Gates

Local sanity:

```bash
uv run python scripts/check_capture_integrity.py
uv run python scripts/check_response_contract_paths.py
uv run --group dev pytest -q
uv run bot contracts --tag programming --no-execute-reads
```

CI workflow (`.github/workflows/contracts.yml`) runs:

- compile check
- capture integrity check
- response-path contract check
- tests
- contract parity checks
- optional live read checks when secrets/vars are configured

## Deployment Readiness Notes

Code and local gates are in place; production readiness still requires:

1. explicit release/scheduling workflow
2. production runbook (including rollback)
3. promotion policy for sustained live scheduling (with optional dry-run preflight gates)

## Repository Layout

```text
src/medium_stealth_bot/
  main.py
  settings.py
  logic.py
  repository.py
  database.py
  operations.py
  contracts.py
  client.py
  safety.py
  timing.py
  observability.py
  artifact_schema.py
  redaction.py
  typed_payloads.py
  migrations/
scripts/
captures/
tests/
```

## References

- [Project-Overview.md](Project-Overview.md)
- [DEVELOPMENT_PLAN.md](DEVELOPMENT_PLAN.md)
- [captures/README.md](captures/README.md)
