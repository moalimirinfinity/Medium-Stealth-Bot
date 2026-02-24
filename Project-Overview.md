# Project Overview: Medium Stealth Bot

## 1. Mission

Medium Stealth Bot is a local-first CLI that automates Medium growth workflows with strong emphasis on:

- behavior realism
- safety controls
- capture-driven API contracts
- simple single-machine operation

The system is intentionally not distributed and does not require external services.

## 2. Current Architecture (2026 Scaffold)

### Runtime Stack

- Python 3.12+
- `uv` + `pyproject.toml` for dependency/runtime management
- `Typer` + `Rich` CLI
- `Pydantic v2` + `pydantic-settings`
- `structlog` JSON logging
- `SQLite` local state
- `Playwright` for interactive auth and stealth execution mode
- `curl-cffi` for fast execution mode

### Dual Client Strategy

1. `CLIENT_MODE=stealth` (default)
   - Uses Playwright persistent browser profile and `APIRequestContext`.
   - Keeps auth and request execution on the same browser/TLS stack.
   - Preferred for production-like runs.

2. `CLIENT_MODE=fast`
   - Uses `curl-cffi` async session with Chrome impersonation.
   - Lower overhead for local dev and test loops.

### Authentication Flow

1. User runs `uv run bot auth`.
2. Playwright opens a headed browser for manual login.
3. Session cookies are extracted and written to `.env`.
4. Persistent browser profile is stored under `.data/playwright-profile`.

## 3. API Contract Source of Truth

Implementation is based on the curated capture pack under `captures/`.

Canonical files:

- `captures/final/live_capture_2026-02-24.json`
- `captures/final/live_ops_2026-02-24.json`
- `captures/final/implementation_ops_2026-02-24.json`

These captures include:

- operation names
- variable shapes
- practical mutation semantics
- confidence markers (`live_ui_observed` vs `probe_stubbed_only`)

## 4. Medium Follow Semantics

Critical behavior mapping currently used by the project:

1. Default follow button path:
   - `SubscribeNewsletterV3Mutation`
   - Classified as `newsletter_subscribe`
2. Notification-off path:
   - `UnsubscribeNewsletterV3Mutation`
   - Classified as `newsletter_unsubscribe`
3. Full graph unfollow path:
   - `UnfollowUserMutation`
   - Classified as `user_unfollow`
4. True follow state verification:
   - `UserViewerEdge.user.viewerEdge.isFollowing`

The bot must not treat newsletter subscription as guaranteed user follow.

## 5. Data Model

Current scaffold initializes these tables:

- `users`
- `relationships`
- `action_log`
- `snapshots`

`action_log` is used for daily budget gating in `bot run`.

## 6. Current CLI Surface

- `uv run bot auth`
  - interactive login + env cookie capture
- `uv run bot probe --tag programming`
  - parallel read-only GraphQL probes
- `uv run bot run --tag programming`
  - budget check + probe scaffold

## 7. Current Status

Implemented and validated:

- auth capture workflow
- dual network client modes
- async batch GraphQL execution
- basic local persistence bootstrap
- capture corpus and implementation notes

Not yet implemented:

- full action engine for follow/unfollow/clap/response orchestration
- reconciliation loops against actual follow state
- robust test suite and CI gates
- production runbook and failure recovery automation

## 8. Next Steps

Execution plan is tracked in `DEVELOPMENT_PLAN.md`.
