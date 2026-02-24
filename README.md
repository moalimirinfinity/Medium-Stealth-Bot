# Medium Stealth Bot (Scaffold)

Python scaffold for a local-first Medium automation toolchain using:

- `uv` for project/venv/lock management
- `pyproject.toml` (PEP 621) as single config
- `Pydantic v2` + `pydantic-settings` for config/data models
- `asyncio` + dual GraphQL clients:
  - `CLIENT_MODE=stealth` (default): Playwright `APIRequestContext` using persistent profile/TLS stack
  - `CLIENT_MODE=fast`: `curl-cffi` async session for lighter/faster iteration
- `structlog` for structured JSON logs
- `Playwright` for interactive auth/session capture
- `Typer` + `Rich` for CLI UX

## Quickstart

```bash
uv sync
cp .env.example .env
uv run bot --help
```

Set `.env` mode:

```bash
# production-like fingerprint alignment
CLIENT_MODE="stealth"

# lighter local iteration
# CLIENT_MODE="fast"
```

## CLI Commands

### 1) Authenticate and capture session
```bash
uv run bot auth
```

This opens a Playwright browser. Log into Medium, then press Enter in terminal.  
By default it updates `.env` with:
- `MEDIUM_SESSION`
- `MEDIUM_CSRF`
- `MEDIUM_USER_REF`

`bot auth` always runs headed for interactive login. Runtime client headless behavior is controlled by `PLAYWRIGHT_HEADLESS`.

### 2) Parallel read-only probe
```bash
uv run bot probe --tag programming
```

Runs parallel GraphQL queries (`TopicLatestStorieQuery`, `WhoToFollowModuleQuery`, `UserViewerEdge` when available, etc.) and prints a Rich table.

### 3) Daily run scaffold
```bash
uv run bot run --tag programming
```

Performs budget check (`action_log` count for current UTC day) then runs probe stage.

## Project Layout

```text
src/medium_stealth_bot/
  main.py         # Typer CLI entrypoint
  settings.py     # Pydantic settings/env model
  models.py       # Pydantic data contracts
  client.py       # Async GraphQL client (playwright-stealth or curl-cffi-fast)
  operations.py   # Captured GraphQL operation builders
  auth.py         # Playwright interactive auth + env writeback
  database.py     # SQLite schema bootstrap
  repository.py   # Action log repository
  logic.py        # Probe + daily cycle orchestration
captures/         # Ground-truth endpoint captures and notes
```
