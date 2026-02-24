# Medium Stealth Bot

Local-first Medium automation scaffold focused on stealth-safe execution and capture-driven API contracts.

## Stack

- `uv` + `pyproject.toml` (single Python project config)
- `Pydantic v2` + `pydantic-settings` (typed env/config)
- `asyncio` orchestration
- Dual network clients:
  - `CLIENT_MODE=stealth` (default): Playwright persistent profile + `APIRequestContext`
  - `CLIENT_MODE=fast`: `curl-cffi` async session (`impersonate="chrome142"`)
- `structlog` JSON logging
- `Typer` + `Rich` CLI
- `SQLite` local state

## Why Two Modes

- `stealth` keeps auth and execution on the same browser stack/fingerprint path.
- `fast` is lighter for local development and dry iteration.

## Quickstart

```bash
uv sync
uv run playwright install chromium
cp .env.example .env
uv run bot --help
```

Then authenticate once:

```bash
uv run bot auth
```

This captures and writes:
- `MEDIUM_SESSION`
- `MEDIUM_CSRF`
- `MEDIUM_USER_REF`

## Main Commands

```bash
uv run bot probe --tag programming
uv run bot contracts --tag programming
uv run bot contracts --tag programming --execute-reads
uv run bot run --tag programming --dry-run
uv run bot run --tag programming --live --seed-user @some_creator
uv run bot reconcile --dry-run
uv run bot artifacts validate
uv run bot status
```

- `probe`: parallel read-only GraphQL checks for current session health.
- `contracts`: validates runtime operation builders against the canonical implementation registry.
  - `--execute-reads` optionally executes read/state-verify operations live (requires `MEDIUM_SESSION`).
  - `--newsletter-slug` and `--newsletter-username` provide a live `NewsletterV3ViewerEdge` input pair.
- `run`: discovery + scoring + eligibility checks + follow pipeline + non-reciprocal cleanup.
  - Candidate sources: topic stories, who-to-follow module, optional followers-of-seed users.
  - Dry-run mode simulates actions and logs decisions without sending follow/unfollow/clap mutations.
  - Live mode executes subscribe/unfollow/clap mutations with immediate follow-state verification.
  - Enforces per-action daily budgets (`subscribe`, `unfollow`, `clap`) plus global daily budget.
  - Uses retry/backoff policy with operation-specific retry limits.
  - Persists candidate reconciliation state for later verification loops.
  - Applies session warm-up + non-uniform action timing and halts on risk signals.
  - Emits machine-readable run artifacts with action/result/reason summaries.
- `reconcile`: scheduled follow-state reconciliation loop using `UserViewerEdge`.
  - Scans persisted candidate/follow-cycle state in pages (`--limit`, `--page-size`).
  - Dry-run validates state without persistence; live mode persists canonical state updates.
- `artifacts validate`: schema/version compatibility checks for run artifacts.
- `status`: prints last-run health diagnostics from the latest run artifact.

## Configuration

Primary runtime env vars are documented in `.env.example`. Key ones:

- `CLIENT_MODE=stealth|fast`
- `DAY_BOUNDARY_POLICY=utc`
- `RUN_ARTIFACTS_DIR=.data/runs`
- `PLAYWRIGHT_PROFILE_DIR=.data/playwright-profile`
- `PLAYWRIGHT_HEADLESS=true|false`
- `MAX_ACTIONS_PER_DAY`
- `MAX_SUBSCRIBE_ACTIONS_PER_DAY`
- `MAX_UNFOLLOW_ACTIONS_PER_DAY`
- `MAX_CLAP_ACTIONS_PER_DAY`
- `MAX_FOLLOW_ACTIONS_PER_RUN`
- `RECONCILE_SCAN_LIMIT`
- `RECONCILE_PAGE_SIZE`
- `FOLLOW_CANDIDATE_LIMIT`
- `FOLLOW_COOLDOWN_HOURS`
- `MIN_FOLLOWING_FOLLOWER_RATIO`
- `SCORE_WEIGHT_RATIO`
- `SCORE_WEIGHT_KEYWORD`
- `SCORE_WEIGHT_SOURCE`
- `SCORE_WEIGHT_NEWSLETTER`
- `BIO_KEYWORDS`
- `DISCOVERY_FOLLOWERS_DEPTH`
- `DISCOVERY_SECOND_HOP_SEED_LIMIT`
- `UNFOLLOW_NONRECIPROCAL_AFTER_DAYS`
- `ENABLE_PRE_FOLLOW_CLAP`
- `MIN_SESSION_WARMUP_SECONDS`
- `MAX_SESSION_WARMUP_SECONDS`
- `RISK_HALT_CONSECUTIVE_FAILURES`
- `ENABLE_CHALLENGE_HALT`
- `CHALLENGE_STATUS_CODES`
- `CHALLENGE_DETECTION_TOKENS`
- `ENABLE_SESSION_EXPIRY_HALT`
- `SESSION_EXPIRY_STATUS_CODES`
- `SESSION_EXPIRY_DETECTION_TOKENS`
- `QUERY_MAX_RETRIES`
- `VERIFY_MAX_RETRIES`
- `MUTATION_MAX_RETRIES`
- `RETRY_BASE_DELAY_SECONDS`
- `RETRY_MAX_DELAY_SECONDS`
- `ADAPTIVE_RETRY_FAILURE_MULTIPLIER`
- `OPERATOR_KILL_SWITCH`
- `GRAPHQL_ENDPOINT`
- `IMPLEMENTATION_OPS_REGISTRY_PATH`
- `CONTRACT_REGISTRY_STRICT`
- `CONTRACT_REGISTRY_VALIDATE_RESPONSE_FIELDS`
- `CONTRACT_REGISTRY_LIVE_NEWSLETTER_SLUG`
- `CONTRACT_REGISTRY_LIVE_NEWSLETTER_USERNAME`
- `MEDIUM_USER_REF=<user_id>` (explicitly a Medium `user_id`, not `@username`)

## Product Rules

- Canonical relationship model is two-dimensional:
  - `newsletter_state`: `subscribed | unsubscribed | unknown`
  - `user_follow_state`: `following | not_following | unknown`
- Daily budget is always computed on UTC calendar boundaries.
- `MEDIUM_USER_REF` is treated as `user_id` only for `UserViewerEdge` verification.
- `--seed-user` accepts `@username` or `user_id` for discovery input; this is separate from `MEDIUM_USER_REF`.
- Newsletter subscribe state is never treated as guaranteed user-follow state.
- `PublishPostThreadedResponse` is kept in the operation registry but intentionally out of the default daily action engine.
- Runtime hard-stops on risk signals:
  - challenge signature detection
  - auth/session-expiry detection
  - consecutive operation-failure threshold
  - operator kill switch (`OPERATOR_KILL_SWITCH=true`)

## Repository Layout

```text
src/medium_stealth_bot/
  __init__.py
  main.py
  settings.py
  models.py
  logging.py
  contract_registry.py
  artifact_schema.py
  client.py
  auth.py
  operations.py
  database.py
  typed_payloads.py
  redaction.py
  repository.py
  observability.py
  safety.py
  timing.py
  logic.py
  migrations/
captures/
  final/
  scripts/
Project-Overview.md
DEVELOPMENT_PLAN.md
```

## Reference Docs

- [Project-Overview.md](Project-Overview.md)
- [DEVELOPMENT_PLAN.md](DEVELOPMENT_PLAN.md)
- [captures/README.md](captures/README.md)
