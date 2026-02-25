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

For scheduled production-style runs:

```bash
cp .env.production.example .env.production
uv run bot profile-validate --env-path .env.production
```

## Core Commands

```bash
uv run bot setup
uv run bot start
uv run bot start --dry-run-first
uv run bot start --quick-live
uv run bot profile-validate --env-path .env.production
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
- `start`: interactive numbered menu (14 options) for running live/dry cycles, reconcile, probe, contracts, status, setup, and auth without typing full commands.
- `start --quick-live`: direct mode that executes live by default, with optional `--dry-run-first`.
- `profile-validate`: validates production guardrails from an env profile file.
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

## Responsible Usage

- You are responsible for complying with Medium terms, rate limits, and applicable law.
- Keep automation conservative and safety-first; do not bypass security or challenge flows.
- Use dry-run/preflight and profile validation before enabling recurring live schedules.
- Never share or commit live session material.

## Public Repo Security Notes

- `.env` is ignored by git; keep all live credentials there only.
- Do not commit real values for `MEDIUM_SESSION`, `MEDIUM_CSRF`, or user identifiers.
- Capture files in `captures/final/` are sanitized for public push; regenerate/sanitize before committing new captures.

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
uv run python scripts/check_capture_sanitization.py
uv run python scripts/check_response_contract_paths.py
uv run --group dev pytest -q
uv run bot contracts --tag programming --no-execute-reads
uv run bot profile-validate --env-path .env.production.example
```

CI workflows:

- `.github/workflows/contracts.yml`
- compile check
- capture integrity check
- capture sanitization check
- response-path contract check
- tests
- contract parity checks
- optional live read checks when secrets/vars are configured
- `.github/workflows/secrets.yml`
- blocking secret scan gate
- `.github/workflows/release.yml`
- tag-triggered release checks + artifact build + GitHub Release

## Deployment Workflows

- Scheduler runner:
  - `scripts/run_daily_live.sh --env-file .env.production --tag programming`
- Cron template:
  - `ops/scheduling/cron.example`
- launchd template:
  - `ops/scheduling/com.mediumstealthbot.daily.plist`
- Local release helper:
  - `scripts/release_local.sh <version>`

Operational docs:

- `docs/SCHEDULING.md`
- `docs/RELEASE.md`
- `docs/RUNBOOK.md`
- `docs/ROLLBACK.md`
- `docs/PROMOTION_POLICY.md`

## Repository Layout

```text
src/medium_stealth_bot/
  main.py
  settings.py
  deployment.py
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
docs/
ops/
captures/
tests/
```

## References

- [Project-Overview.md](Project-Overview.md)
- [DEVELOPMENT_PLAN.md](DEVELOPMENT_PLAN.md)
- [captures/README.md](captures/README.md)
- [docs/SCHEDULING.md](docs/SCHEDULING.md)
- [docs/RELEASE.md](docs/RELEASE.md)
- [docs/RUNBOOK.md](docs/RUNBOOK.md)
- [docs/ROLLBACK.md](docs/ROLLBACK.md)
- [docs/PROMOTION_POLICY.md](docs/PROMOTION_POLICY.md)
