# Contributing

## Prerequisites

- Python 3.12+
- `uv`
- Chromium for Playwright:
  - `uv run playwright install chromium`

## Setup

```bash
uv sync --group dev
cp .env.example .env
```

## Development Workflow

1. Create a branch from `main`.
2. Make focused commits.
3. Run local checks before opening a PR.

## Local Quality Checks

```bash
uv run python -m compileall -q src
uv run python scripts/check_capture_integrity.py
uv run python scripts/check_capture_sanitization.py
uv run python scripts/check_response_contract_paths.py
uv run pytest -q
uv run bot contracts --tag programming --no-execute-reads
```

## Pull Requests

- keep PR scope tight
- include behavior/testing notes
- update docs when command/interface behavior changes
- avoid introducing secrets or personal account data in tracked files

## Commit Style

Preferred conventional-style prefixes:

- `feat:`
- `fix:`
- `docs:`
- `chore:`
- `test:`

## Safety Expectations

This project has live automation pathways. Contributions must preserve:

- explicit guardrails
- kill-switch behavior
- deterministic dry-run/live distinction
