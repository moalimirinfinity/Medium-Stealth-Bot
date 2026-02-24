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
- `relationships` (legacy compatibility source)
- `relationship_state` (canonical runtime state model)
- `follow_cycle` (follow-back grace tracking and cleanup state)
- `blacklist` (hard block list for candidate safety checks)
- `action_log`
- `snapshots`

Canonical relationship state columns:

- `newsletter_state`: `subscribed | unsubscribed | unknown`
- `user_follow_state`: `following | not_following | unknown`
- `confidence`: `observed | inferred | stubbed`
- `last_source_operation`, `updated_at`, `last_verified_at`

Migration policy:

- DB schema is versioned using `PRAGMA user_version`.
- Existing `relationships.state` values are migrated into `relationship_state.user_follow_state` as inferred values.

`action_log` is used for daily budget gating in `bot run`.

## 6. Product Rules

1. Daily budget boundary is UTC calendar day only (`DAY_BOUNDARY_POLICY=utc`).
2. `MEDIUM_USER_REF` contract is `user_id` only (not `@username`).
3. Newsletter state and user-follow state are tracked separately.
4. `user_follow` reporting is valid only after `UserViewerEdge.isFollowing` verification.

## 7. Current CLI Surface

- `uv run bot auth`
  - interactive login + env cookie capture
- `uv run bot probe --tag programming`
  - parallel read-only GraphQL probes
- `uv run bot contracts --tag programming`
  - contract-registry parity checks
- `uv run bot run --tag programming`
  - full daily cycle (discovery, scoring, follow pipeline, cleanup)
  - supports `--dry-run/--live` and repeated `--seed-user` inputs
- `uv run bot status`
  - last-run diagnostics from `.data/runs/latest.json`

## 8. Current Status

Implemented and validated:

- auth capture workflow
- dual network client modes
- async batch GraphQL execution
- candidate discovery from topic feeds, who-to-follow module, and optional seed followers/followers-of-followers source
- scoring/filtering pipeline (ratio + keyword + cooldown + blacklist + live follow-state check)
- dry-run and live follow pipeline with post-action verification
- non-reciprocal cleanup pass with grace-window state tracking
- per-action daily budget partitioning (`subscribe`, `unfollow`, `clap`) with global UTC budget gate
- operation-specific retry/backoff policy (query/verify/mutation tiers)
- centralized timing controls for session warm-up, read delays, and inter-action gaps
- hard-stop safety guardrails for repeated failures, challenge signals, and session-expiry signals
- structured observability events (`run_id`, `operation`, `target_id`, `decision`, `result`)
- run artifacts under `.data/runs/` plus `bot status` diagnostics command
- expanded local persistence and migration bootstrap (`relationship_state`, `follow_cycle`, `blacklist`)
- capture corpus and implementation notes

Not yet implemented:

- adaptive weight tuning and source performance optimization
- full reconciliation loops against actual follow state across larger paging windows
- robust test suite and CI gates
- production runbook and failure recovery automation

## 9. Next Steps

Execution plan is tracked in `DEVELOPMENT_PLAN.md`.

## 10. Feature Roadmap (Practical Priority Order)

### P1: Core Growth Engine (Must-Have)

1. Candidate discovery from:
   - topic/tag feeds
   - followers of seed users
   - followers-of-followers expansion
2. Candidate enrichment:
   - `followers_count`, `following_count`
   - profile bio keywords (e.g., coding/software/engineering terms)
   - recency/activity hints from feed presence
3. Candidate scoring:
   - weighted ratio signal (followers-to-following quality)
   - weighted relevance signal from bio/topic terms
   - weighted network proximity signal (distance from seed sources)
4. Eligibility filters:
   - skip already-followed users
   - skip recently actioned users (cooldown/idempotency)
   - skip blocked/blacklisted users
5. Follow execution + verification:
   - execute follow path mutation
   - verify with `UserViewerEdge.isFollowing`
   - persist canonical `relationship_state`
6. Follow-back tracking:
   - track whether target follows back within configurable window (`N` days)
7. Non-reciprocal cleanup:
   - unfollow users who did not follow back after grace period
   - honor whitelist/exemptions
8. Safety envelope:
   - daily/hourly action caps
   - randomized delays and cooldown gaps
   - hard-stop on repeated failures/challenge signals

### P2: Quality and Control Layer

1. Campaigns:
   - run targeted sources like "followers of `<seed_user>`" with per-campaign limits
2. Configurable weighting profiles:
   - profile-level keyword packs (`coding`, `software`, `ai`, etc.)
3. Retry/error strategy:
   - classify transient vs hard failures
   - bounded retries with backoff
4. State reconciliation jobs:
   - periodic re-checks to correct local state drift
5. Whitelist/blacklist management:
   - with reason metadata and optional expiry

### P3: Optimization and Operations

1. KPI reporting:
   - follow-back rate
   - source conversion rate
   - net daily/weekly growth
2. Source ranking:
   - score seed users/tags by downstream follow-back quality
3. Adaptive tuning:
   - iteratively tune discovery/scoring weights based on outcomes
4. Dry-run simulation:
   - full decision trace without write mutations
5. Operational safeguards:
   - kill switch and anomaly alerts for error spikes
