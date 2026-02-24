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
uv run bot run --tag programming
```

- `probe`: parallel read-only GraphQL checks for current session health.
- `run`: budget gate + probe scaffold for daily execution.

## Configuration

Primary runtime env vars are documented in `.env.example`. Key ones:

- `CLIENT_MODE=stealth|fast`
- `PLAYWRIGHT_PROFILE_DIR=.data/playwright-profile`
- `PLAYWRIGHT_HEADLESS=true|false`
- `MAX_ACTIONS_PER_DAY`
- `GRAPHQL_ENDPOINT`

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
