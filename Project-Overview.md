# Project Overview: Medium Stealth Bot

## 1. Mission

Medium Stealth Bot is a local-first CLI for Medium growth automation with three core principles:

1. safety-first execution
2. capture-driven API contracts
3. inspectable local state and diagnostics

The system is intentionally single-machine and does not depend on hosted orchestration.

## 2. Architecture Snapshot

### Runtime Stack

- Python 3.12+
- `uv` project/runtime management
- `Typer` + `Rich` CLI
- `Pydantic v2` + `pydantic-settings`
- `structlog` logging (`pretty` console default, optional JSON mode)
- `SQLite` persistence
- `Playwright` + `curl-cffi` clients

### Dual Client Modes

1. `CLIENT_MODE=stealth` (default)
   - Playwright persistent profile + `APIRequestContext`
   - preferred for production-like behavior and session continuity
2. `CLIENT_MODE=fast`
   - async `curl-cffi` session with Chrome impersonation
   - optimized for lower-overhead local iteration

### Auth Bootstrap

1. Run `uv run bot auth`.
2. Complete login in headed Playwright session.
3. Persist session material to `.env`.
4. Reuse browser profile under `.data/playwright-profile`.

## 3. Contract Source of Truth

Implementation contracts are derived from capture artifacts in `captures/final/`, primarily:

- `live_capture_2026-02-24.json`
- `live_ops_2026-02-24.json`
- `implementation_ops_2026-02-24.json`

The implementation registry defines:

- operation set and variable requirements
- classification labels (`read`, `mutation`, `state-verify`, `high-risk`)
- expected response field paths for strict validation

## 4. Follow Semantics Model

Observed Medium semantics used by runtime:

1. subscribe path: `SubscribeNewsletterV3Mutation`
2. unsubscribe notifications path: `UnsubscribeNewsletterV3Mutation`
3. graph unfollow path: `UnfollowUserMutation`
4. canonical follow-state verification: `UserViewerEdge.user.viewerEdge.isFollowing`

Design rule: newsletter subscription is not treated as guaranteed user-follow state.

## 5. Local Data Model

### Core Tables

- `users`
- `relationships` (legacy compatibility table)
- `relationship_state` (canonical)
- `follow_cycle`
- `candidate_reconciliation`
- `blacklist`
- `own_followers_cache`
- `own_following_cache`
- `graph_sync_runs`
- `graph_sync_state`
- `action_log`
- `snapshots`
- `schema_migrations`

### Canonical Relationship State

- `newsletter_state`: `subscribed | unsubscribed | unknown`
- `user_follow_state`: `following | not_following | unknown`
- `confidence`: `observed | inferred | stubbed`

### Migration Strategy

- Numbered SQL files in `src/medium_stealth_bot/migrations/`
- Migration history tracked in `schema_migrations`
- `PRAGMA user_version` synchronized to latest applied migration version
- Migration checksum validation prevents silent drift

## 6. Runtime Behavior

### Daily Run (`bot run`)

Pipeline stages:

1. probe
2. candidate discovery
3. scoring
4. eligibility filtering
5. follow/subscribe attempt (or dry-run planning)
6. verification
7. state persistence
8. optional cleanup of due non-reciprocal follows

Discovery sources:

- topic latest stories
- topic who-to-follow
- who-to-follow module
- optional seed followers (plus optional second hop)

### Reconciliation (`bot reconcile`)

- Builds a paginated worklist from `candidate_reconciliation` and pending `follow_cycle` users.
- Executes `UserViewerEdge` checks.
- Writes canonical follow-state updates in live mode.

### Social Graph Sync (`bot sync`)

- Pulls own followers and following into local cache tables.
- Uses full pagination by default (`--full` default true).
- Supports force refresh (`--force`) that bypasses freshness window checks.
- Upserts `users` from cache snapshots and imports missing following rows into `follow_cycle` as pending.

### Cleanup (`bot cleanup`)

- Uses cache-first candidate filtering: only pending rows that still exist in `own_following_cache` are unfollow-eligible.
- Missing-cache rows are marked skipped (`cleanup_status='skipped'`) in live mode to prevent repeated churn.
- Applies whitelist keep logic when follower count meets `CLEANUP_UNFOLLOW_WHITELIST_MIN_FOLLOWERS`.
- Uses short unfollow pacing gaps clamped to effective `1-4s`.

### Safety

Hard-stop triggers:

- challenge detection (status/text signatures)
- session/auth expiry signatures
- consecutive failure threshold
- operator kill switch (`OPERATOR_KILL_SWITCH=true`)

### Timing

- session warm-up sleep
- read-delay sleeps
- inter-action non-uniform cooldowns
- cleanup-only short unfollow pacing window (`1-4s` effective clamp)

## 7. CLI Surface

- `uv run bot setup`
- `uv run bot start` (interactive numbered menu)
- `uv run bot start --quick-live [--dry-run-first]`
- `uv run bot profile-validate --env-path .env.production`
- `uv run bot auth`
- `uv run bot probe --tag programming`
- `uv run bot contracts --tag programming [--execute-reads]`
- `uv run bot run --tag programming [--dry-run] [--seed-user ...]`
- `uv run bot cleanup [--live|--dry-run] [--limit N]`
- `uv run bot sync [--live|--dry-run] [--force] [--full|--respect-pagination-config]`
- `uv run bot reconcile --limit N --page-size N [--dry-run]`
- `uv run bot artifacts validate [--path <artifact>]`
- `uv run bot status`

### Start Menu Grouping

When you run `uv run bot start`, options are grouped and ordered as:

- `1-4`: execution
- `5-9`: maintenance
- `10-12`: diagnostics
- `13-14`: observability
- `15-16`: config
- `17`: auth
- `18`: system/exit

## 8. Operational Contracts

1. UTC day boundary is mandatory for budget accounting.
2. `MEDIUM_USER_REF` is `user_id` only.
3. State verification and mutation side effects are recorded independently.
4. `PublishPostThreadedResponse` remains in contract coverage, but is intentionally excluded from default daily execution flow.

## 9. Quality and Validation

Current baseline includes:

- contract parity checks
- strict response-path checks
- capture freshness/integrity checks
- capture sanitization checks
- unit/integration-style local tests
- CI quality workflow in `.github/workflows/contracts.yml`
- CI secret scanning workflow in `.github/workflows/secrets.yml`
- tag-triggered release workflow in `.github/workflows/release.yml`

Optional CI live-read validation runs when required secrets/variables are configured.

## 10. Current Maturity

### Implemented

- phases 0 through 7 from the development plan
- live-read strict contract checks with newsletter slug + username inputs
- file-based migrations, idempotency keys, reconciliation persistence
- safety guardrails, run artifacts, status diagnostics, redaction layer
- production profile validation + scheduler templates
- release automation + security scanning + runbook/rollback/promotion docs
