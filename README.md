# Medium Stealth Bot

Local-first Medium automation scaffold focused on stealth-safe execution and capture-driven API contracts.

## Stack

- `uv` + `pyproject.toml` (single Python project config)
- `Pydantic v2` + `pydantic-settings` (typed env/config)
- `asyncio` orchestration
- Dual network clients:
  - `CLIENT_MODE=stealth` (default): Playwright persistent profile + `APIRequestContext`
  - `CLIENT_MODE=fast`: `curl-cffi` async session (`impersonate="chrome120"`)
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
uv run bot run --tag programming --dry-run
uv run bot run --tag programming --live --seed-user @some_creator
```

- `probe`: parallel read-only GraphQL checks for current session health.
- `run`: discovery + scoring + eligibility checks + follow pipeline + non-reciprocal cleanup.
  - Candidate sources: topic stories, who-to-follow module, optional followers-of-seed users.
  - Dry-run mode simulates actions and logs decisions without sending follow/unfollow/clap mutations.
  - Live mode executes subscribe/unfollow/clap mutations with immediate follow-state verification.

## Configuration

Primary runtime env vars are documented in `.env.example`. Key ones:

- `CLIENT_MODE=stealth|fast`
- `DAY_BOUNDARY_POLICY=utc`
- `PLAYWRIGHT_PROFILE_DIR=.data/playwright-profile`
- `PLAYWRIGHT_HEADLESS=true|false`
- `MAX_ACTIONS_PER_DAY`
- `MAX_FOLLOW_ACTIONS_PER_RUN`
- `FOLLOW_CANDIDATE_LIMIT`
- `FOLLOW_COOLDOWN_HOURS`
- `MIN_FOLLOWING_FOLLOWER_RATIO`
- `BIO_KEYWORDS`
- `DISCOVERY_FOLLOWERS_DEPTH`
- `DISCOVERY_SECOND_HOP_SEED_LIMIT`
- `UNFOLLOW_NONRECIPROCAL_AFTER_DAYS`
- `ENABLE_PRE_FOLLOW_CLAP`
- `GRAPHQL_ENDPOINT`
- `MEDIUM_USER_REF=<user_id>` (explicitly a Medium `user_id`, not `@username`)

## Product Rules

- Canonical relationship model is two-dimensional:
  - `newsletter_state`: `subscribed | unsubscribed | unknown`
  - `user_follow_state`: `following | not_following | unknown`
- Daily budget is always computed on UTC calendar boundaries.
- `MEDIUM_USER_REF` is treated as `user_id` only for `UserViewerEdge` verification.
- Newsletter subscribe state is never treated as guaranteed user-follow state.

## Repository Layout

```text
src/medium_stealth_bot/
  main.py
  settings.py
  models.py
  client.py
  auth.py
  operations.py
  database.py
  repository.py
  logic.py
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
