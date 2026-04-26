# Medium Stealth Bot

Local-first Medium automation with explicit discovery, queue-driven growth execution, unfollow maintenance, and inspectable run artifacts.

## Current Project State

The project is now organized around separate operational pipelines:

- `Discovery`: collect, score, filter, and persist execution-ready candidates into the local queue
- `Growth`: drain the execution-ready queue using `follow-only`, `warm-engage`, or `warm-engage-plus-rare-comment`
- `Unfollow`: run cleanup-only unfollow workflows
- `Maintenance`: sync local graph state, reconcile follow state, and prune stale DB rows
- `Diagnostics` / `Observability`: contract checks, probes, queue status, and artifact validation

This is a local operator workflow. Auth, DB, queue, browser profile, and run artifacts stay on the machine.

## Feature Highlights

- Interactive start menu with section-first navigation
- Queue-first growth model: discovery chooses candidates, growth executes actions
- Separate unfollow/cleanup workflow
- Local SQLite persistence for queue, follow cycles, reconciliation, and graph cache
- Contract-aware GraphQL client and capture-backed registry
- Live and dry-run modes across discovery, growth, cleanup, and maintenance
- Run artifacts, queue status views, DB hygiene tooling, and artifact validation
- Production profile validation and local scheduler support

## Quick Start

```bash
uv sync --group dev
uv run playwright install chromium
cp .env.example .env
uv run bot setup
uv run bot start
```

`bot setup` can help import auth and write baseline defaults to `.env`.

## Recommended Workflow

1. Validate your profile.

```bash
cp .env.production.example .env.production
uv run bot profile-validate --env-path .env.production
```

2. Fill the queue with discovery.

```bash
uv run bot discover --live --source topic-recommended --source seed-followers --tag programming
```

3. Check queue status.

```bash
uv run bot queue
```

4. Run growth against the ready queue.

```bash
uv run bot run --policy warm-engage --session
```

5. Inspect status and validate the latest artifact.

```bash
uv run bot status
uv run bot observe validate-artifact
```

## Core Commands

### Setup and auth

```bash
uv run bot setup
uv run bot auth
uv run bot auth-import --cookie-header "sid=...; uid=...; xsrf=..."
uv run bot start
```

### Discovery

```bash
uv run bot discover --live --source topic-recommended --source seed-followers --tag programming
uv run bot discover --dry-run --source responders --tag programming
uv run bot discover --live --source target-user-followers --target-user @username
uv run bot growth followers --target-user @username --live
uv run bot growth followers --target-user id:USER_ID --scan-limit 200 --dry-run
```

### Growth execution

```bash
uv run bot run --policy follow-only --single-cycle
uv run bot run --policy warm-engage --session
uv run bot run --policy warm-engage-plus-rare-comment --dry-run --single-cycle
uv run bot growth cycle --policy warm-engage --live
uv run bot growth session --policy warm-engage --session-minutes 90 --target-follows 120
uv run bot growth cycle --policy follow-only --dry-run
uv run bot growth preflight --policy follow-only
```

Notes:

- `run` is growth execution only. Discovery inputs such as `--source`, `--seed-user`, and `--target-user` are kept only as legacy compatibility flags and are ignored there.
- Combined discovery+growth cycles are rejected at runtime; run `bot discover` first, then `bot run`.
- `growth cycle --dry-run` is the grouped alias for a dry-run single pass.
- `growth preflight` currently runs a dry-run single pass and then continues into a live growth session, matching quick-live hybrid behavior.
- Discovery targets `DISCOVERY_ELIGIBLE_PER_RUN` execution-ready candidates per live run, default `100`, while respecting `GROWTH_CANDIDATE_QUEUE_MAX_SIZE`, default `700`.
- Candidate eligibility belongs to discovery. Growth trusts ready queue rows and does not re-score or re-filter by ratio, follower counts, bio, keywords, latest post, or recent activity.
- Candidate ranking stores a score breakdown with follow-back likelihood, topic affinity, source quality, newsletter availability, Medium presence, activity, penalties, and conservative learning adjustments.
- The growth queue is kept actionable: discovery stores only `eligible:execution_ready` candidates, live runs purge non-actionable legacy rows, and verified follows remove the candidate row.
- If the queue has no ready candidates, run discovery first.

### Queue and observability

```bash
uv run bot queue
uv run bot observe queue
uv run bot status
uv run bot observe validate-artifact
```

### Maintenance and unfollow

```bash
uv run bot sync --live --force
uv run bot reconcile --dry-run --limit 200 --page-size 50
uv run bot cleanup --dry-run --limit 50
uv run bot cleanup --live --limit 50
uv run bot maintenance db-hygiene --dry-run
uv run bot maintenance db-hygiene --live --vacuum
```

### Diagnostics

```bash
uv run bot probe --tag programming
uv run bot contracts --tag programming --no-execute-reads
uv run bot contracts --tag programming --execute-reads \
  --newsletter-slug "$CONTRACT_REGISTRY_LIVE_NEWSLETTER_SLUG" \
  --newsletter-username "$CONTRACT_REGISTRY_LIVE_NEWSLETTER_USERNAME"
```

## Start Menu

`uv run bot start` opens the current section-first menu:

| Option | Section | Purpose |
| --- | --- | --- |
| 1 | Discovery | Discover, score, evaluate, and queue growth candidates. |
| 2 | Growth | Execute follow growth from the execution-ready queue. |
| 3 | Unfollow | Run cleanup-only unfollow workflows. |
| 4 | Maintenance | Sync, reconcile, and DB hygiene operations. |
| 5 | Diagnostics | Probe reads and validate contracts. |
| 6 | Observability | Inspect latest run status, queue, and artifacts. |
| 7 | Settings/Auth | Edit defaults, run setup, or refresh auth. |
| 8 | Exit | Leave the interactive menu. |

Discovery sources exposed in the menu:

- `Topic/Recommended`
- `Seed Followers`
- `Target-User Followers`
- `Publication/Adjacency`
- `Responders`

Growth policies exposed in the menu:

- `Follow-Only`
- `Warm-Engage`
- `Warm-Engage++`

Growth runtimes exposed in the menu:

- `Session`
- `Single Pass`
- `Preflight` (dry-run single pass)
- `Hybrid` (dry-run single pass, then live session)

## Grouped Aliases

The top-level commands remain available. Grouped aliases mirror the current menu structure:

```bash
uv run bot growth discover --source topic-recommended --tag programming
uv run bot growth queue
uv run bot growth session --policy warm-engage
uv run bot growth cycle --policy follow-only --dry-run
uv run bot growth preflight --policy warm-engage
uv run bot unfollow cleanup --live --limit 50
uv run bot maintenance sync --force
uv run bot maintenance reconcile --dry-run --limit 200 --page-size 50
uv run bot maintenance db-hygiene --dry-run
uv run bot diagnostics probe --tag programming
uv run bot diagnostics contracts --tag programming --no-execute-reads
uv run bot observe status
uv run bot observe validate-artifact
```

## Safety and Guardrails

- UTC day-boundary accounting for budgets
- Dry-run and live modes are explicit in discovery, growth, cleanup, and maintenance
- Discovery applies candidate screening before enqueueing; growth only executes selected queue rows
- Discovery persists score explanations for queued candidates and follow-cycle learning
- Growth re-checks live follow state right before mutation to avoid following someone already followed
- Verified follows and already-following action guards remove candidates from the growth queue
- Safety halts can stop runs on challenge signatures, auth/session-expiry signals, repeated failures, or operator kill switch
- Queue artifacts and status output expose ready and deferred counts
- DB hygiene provides controlled pruning for stale operational data

## Key Configuration Areas

Start from [.env.example](/Users/moalimir/Project%20World/Medium-Stealth-Bot/.env.example) and tune:

- Auth/session: `MEDIUM_SESSION*`, `MEDIUM_CSRF`, `MEDIUM_USER_REF`
- Growth defaults: `DEFAULT_GROWTH_POLICY`, `DEFAULT_GROWTH_SOURCES`
- Discovery/scoring: `DISCOVERY_ELIGIBLE_PER_RUN`, `GROWTH_CANDIDATE_QUEUE_MAX_SIZE`, queue buffer targets, candidate bounds, ratio thresholds, layered topic keywords, negative keyword penalty/rejection, source-quality overrides, adaptive learning settings, and follow-back/topic/source/newsletter/presence/activity score weights
- Session targets: `LIVE_SESSION_DURATION_MINUTES`, `LIVE_SESSION_TARGET_FOLLOW_ATTEMPTS`, `LIVE_SESSION_MIN_FOLLOW_ATTEMPTS`, `LIVE_SESSION_MAX_PASSES`
- Pacing: read delay, verify gap, action gap, pass cooldown, mutation window, session warmup
- Graph sync: auto-sync, freshness window, GraphQL following, scrape fallback
- Cleanup: nonreciprocal window, cleanup limit, whitelist threshold
- Queue and DB hygiene: queue retry/prune windows, operational retention windows, and optional post-cleanup vacuum

## Docs

- [Project-Overview.md](/Users/moalimir/Project%20World/Medium-Stealth-Bot/Project-Overview.md)
- [docs/RUNBOOK.md](/Users/moalimir/Project%20World/Medium-Stealth-Bot/docs/RUNBOOK.md)
- [docs/SCHEDULING.md](/Users/moalimir/Project%20World/Medium-Stealth-Bot/docs/SCHEDULING.md)
- [docs/PROMOTION_POLICY.md](/Users/moalimir/Project%20World/Medium-Stealth-Bot/docs/PROMOTION_POLICY.md)
- [docs/ROLLBACK.md](/Users/moalimir/Project%20World/Medium-Stealth-Bot/docs/ROLLBACK.md)
- [docs/RELEASE.md](/Users/moalimir/Project%20World/Medium-Stealth-Bot/docs/RELEASE.md)
- [captures/README.md](/Users/moalimir/Project%20World/Medium-Stealth-Bot/captures/README.md)
- [captures/IMPLEMENTATION_NOTES.md](/Users/moalimir/Project%20World/Medium-Stealth-Bot/captures/IMPLEMENTATION_NOTES.md)
