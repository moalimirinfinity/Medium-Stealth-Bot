# Medium Stealth Bot

Automate Medium growth with a local-first, safety-guarded engine built for repeatable live execution.

This project turns manual follower discovery, scoring, follow/reconcile cycles, and diagnostics into a structured CLI workflow with strict risk controls.

## Why This Project

- Runs real growth loops with enforceable budgets and timing gates.
- Keeps control local: local auth, local DB, local artifacts, local scheduler.
- Uses capture-driven GraphQL contracts to detect drift before it breaks runs.
- Ships with deployment hardening for public OSS and daily production-style usage.

## Feature Highlights

- Interactive CLI menu (`bot start`) with guided options for live, dry-run, reconcile, contracts, and status.
- Quick-live mode (`bot start --quick-live`) for automation and scheduler use.
- Contract integrity layer:
  - operation parity checks
  - response field/path validation
  - optional live read verification
- Safety model:
  - challenge/session-expiry halt detection
  - consecutive-failure halts
  - operator kill switch
- Local observability:
  - versioned run artifacts
  - artifact schema validation
  - latest-run diagnostics (`bot status`)
- Deployment tooling:
  - production profile validator
  - daily scheduler runner
  - release helper + release workflow
  - secret scanning gate

## Quick Start

```bash
uv sync --group dev
uv run playwright install chromium
cp .env.example .env
uv run bot setup
uv run bot start
```

`bot setup` can capture auth (if needed) and write sane defaults to `.env`.

## Live Run Flow (Recommended)

1. Prepare a production profile.

```bash
cp .env.production.example .env.production
uv run bot profile-validate --env-path .env.production
```

2. Run a preflight + live cycle.

```bash
uv run bot start --quick-live --dry-run-first --tag programming
```

3. Check diagnostics.

```bash
uv run bot status
uv run bot artifacts validate
```

## Core Commands

```bash
uv run bot setup
uv run bot auth
uv run bot start
uv run bot start --quick-live
uv run bot start --quick-live --dry-run-first --tag programming
uv run bot run --tag programming
uv run bot run --dry-run --tag programming
uv run bot reconcile --limit 200 --page-size 50
uv run bot reconcile --dry-run --limit 200 --page-size 50
uv run bot probe --tag programming
uv run bot contracts --tag programming --no-execute-reads
uv run bot contracts --tag programming --execute-reads \
  --newsletter-slug "$CONTRACT_REGISTRY_LIVE_NEWSLETTER_SLUG" \
  --newsletter-username "$CONTRACT_REGISTRY_LIVE_NEWSLETTER_USERNAME"
uv run bot profile-validate --env-path .env.production
uv run bot status
uv run bot artifacts validate
```

## Safety and Guardrails

- UTC day-boundary policy for all daily budgets.
- `MEDIUM_USER_REF` must be a Medium `user_id` (not `@username`).
- Live halts on:
  - challenge detections/status codes
  - session-expiry/auth failure signatures
  - consecutive failure threshold
  - `OPERATOR_KILL_SWITCH=true`
- Dry-run and live paths are explicit and auditable in artifacts.

## Key Configuration

Start from `.env.example` and tune:

- runtime:
  - `CLIENT_MODE`
  - `DAY_BOUNDARY_POLICY`
  - `LOG_FORMAT`
- budgets:
  - `MAX_ACTIONS_PER_DAY`
  - `MAX_FOLLOW_ACTIONS_PER_RUN`
  - `MAX_SUBSCRIBE_ACTIONS_PER_DAY`
  - `MAX_UNFOLLOW_ACTIONS_PER_DAY`
  - `MAX_CLAP_ACTIONS_PER_DAY`
- pacing:
  - `MIN_READ_WAIT_SECONDS`
  - `MIN_ACTION_GAP_SECONDS`
- safety:
  - `RISK_HALT_CONSECUTIVE_FAILURES`
  - `ENABLE_CHALLENGE_HALT`
  - `ENABLE_SESSION_EXPIRY_HALT`
  - `OPERATOR_KILL_SWITCH`
- contracts:
  - `CONTRACT_REGISTRY_VALIDATE_RESPONSE_FIELDS`
  - `CONTRACT_REGISTRY_LIVE_NEWSLETTER_SLUG`
  - `CONTRACT_REGISTRY_LIVE_NEWSLETTER_USERNAME`

## Quality Gates

Local checks:

```bash
uv run python -m compileall -q src
uv run python scripts/check_capture_integrity.py
uv run python scripts/check_capture_sanitization.py
uv run python scripts/check_response_contract_paths.py
uv run --group dev pytest -q
uv run bot contracts --tag programming --no-execute-reads
uv run bot profile-validate --env-path .env.production.example
```

CI:

- quality checks: `.github/workflows/contracts.yml`
- secret scanning: `.github/workflows/secrets.yml`
- release workflow: `.github/workflows/release.yml`

## Deployment and Release

- Daily local scheduler runner:
  - `scripts/run_daily_live.sh --env-file .env.production --tag programming`
- Schedule templates:
  - `ops/scheduling/cron.example`
  - `ops/scheduling/com.mediumstealthbot.daily.plist`
- Local release helper:
  - `scripts/release_local.sh <version>`

## Responsible Usage

- You are responsible for compliance with Medium policy, rate limits, and applicable law.
- Do not attempt to bypass challenges or account protections.
- Never commit live credentials or raw cookie material.
- Captures in `captures/final/` must remain sanitized for public pushes.

## Repo Map

```text
src/medium_stealth_bot/   core application
scripts/                  checks, scheduler, release helpers
captures/                 capture corpus + manifest + capture docs
ops/scheduling/           cron/launchd templates
tests/                    test suite
```

## References

- [Project-Overview.md](Project-Overview.md)
- [captures/README.md](captures/README.md)
