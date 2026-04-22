# Medium Stealth Bot

Automate Medium growth with a local-first, safety-guarded engine built for repeatable live execution.

This project turns manual follower discovery, scoring, follow/reconcile cycles, and diagnostics into a structured CLI workflow with operator-defined risk controls.

## Why This Project

- Runs real growth loops with enforceable budgets and timing gates.
- Keeps control local: local auth, local DB, local artifacts, local scheduler.
- Uses capture-driven GraphQL contracts to detect drift before it breaks runs.
- Ships with deployment hardening for public OSS and daily production-style usage.

## Feature Highlights

- Interactive CLI menu (`bot start`) with section-first navigation (Growth, Unfollow, Maintenance, Diagnostics, Observability, Settings/Auth).
- Quick-live mode (`bot start --quick-live`) for automation and scheduler use.
- Grouped command aliases for phase-oriented workflows (`bot growth ...`, `bot unfollow ...`, `bot maintenance ...`, `bot diagnostics ...`, `bot observe ...`).
- Growth and cleanup now run as separate workflows; growth commands no longer execute cleanup inline.
- Live session mode for multi-cycle execution (target duration + follow target) with `LIVE_SESSION_MAX_PASSES` as a hard session cap.
- Cleanup whitelist guard (`CLEANUP_UNFOLLOW_WHITELIST_MIN_FOLLOWERS`) to keep high-follower accounts.
- Cleanup pipeline is cache-first and unfollows only users present in `own_following_cache`; stale pending rows are marked skipped.
- One-click social graph sync (`bot sync`) imports full followers/following snapshots into cache tables and upserts users.
- Reconcile now scans paginated worklists across offsets to avoid first-page-only drift.
- Contract integrity layer:
  - operation parity checks
  - response field/path validation
  - optional live read verification
- Safety model:
  - optional challenge/session-expiry halt detection
  - configurable consecutive-failure halts
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

`profile-validate` now performs baseline profile checks only. Safety thresholds and halt behavior are controlled by your `.env` values.

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
uv run bot auth-import --cookie-header "sid=...; uid=...; xsrf=..."
uv run bot start
uv run bot start --quick-live
uv run bot start --quick-live --dry-run-first --tag programming
uv run bot start --quick-live --policy follow-only --source topic-recommended --tag programming
uv run bot growth session --tag programming
uv run bot growth session --policy warm-engage --source topic-recommended --tag programming
uv run bot growth cycle --dry-run --tag programming
uv run bot growth cycle --policy follow-only --source seed-followers --seed-user @username --dry-run --tag programming
uv run bot growth preflight --policy warm-engage-plus-rare-comment --source responders --tag programming
uv run bot growth followers --target-user @username --dry-run --single-cycle
uv run bot unfollow cleanup --live --limit 50
uv run bot maintenance sync --force
uv run bot maintenance reconcile --dry-run --limit 200 --page-size 50
uv run bot diagnostics probe --tag programming
uv run bot diagnostics contracts --tag programming --no-execute-reads
uv run bot observe status
uv run bot run --tag programming
uv run bot run --tag programming --session --session-minutes 60 --target-follows 100
uv run bot run --tag programming --single-cycle
uv run bot run --dry-run --tag programming
uv run bot cleanup --dry-run
uv run bot cleanup --live --limit 50
uv run bot sync --live
uv run bot sync --dry-run
uv run bot sync --live --force
uv run bot sync --respect-pagination-config
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

One-click full social graph import (followers + following into DB caches, with full pagination):

```bash
uv run bot sync
```

Force refresh even inside freshness window:

```bash
uv run bot sync --force
```

## Auth Fallback (`auth-import`)

If interactive auth is blocked (for example, Google sign-in says browser/app is not secure), import cookies from an already signed-in browser session:

```bash
uv run bot auth-import --cookie-header "sid=...; uid=...; xsrf=..."
# or
uv run bot auth-import --cookie-file /path/to/medium-cookie-header.txt
```

You can copy a Cookie header from browser DevTools Network tab for a signed-in `https://medium.com` request.

## Start Menu (`uv run bot start`)

The interactive menu now opens into a small set of sections first, then shows growth as 3 explicit axes:
- source
- policy
- runtime

The growth source menu supports topic/recommended discovery, seed followers, target-user followers, publication/adjacency discovery, and responders.
The policy menu separates follow-only from warm-engage and warm-engage-plus-rare-comment behavior.

### Top-Level Sections

| Option | Section | Purpose |
| --- | --- | --- |
| 1 | Growth | Run follower-growth workflows. |
| 2 | Unfollow | Run cleanup-only unfollow workflows. |
| 3 | Maintenance | Reconcile local follow state and sync the social graph cache. |
| 4 | Diagnostics | Probe Medium reads and validate operation contracts. |
| 5 | Observability | Inspect latest run status and validate artifacts. |
| 6 | Settings/Auth | Edit defaults, run setup, or refresh auth. |
| 7 | Exit | Leave the interactive start menu. |

### Section Workflows

| Section | Options |
| --- | --- |
| Growth | choose source, choose policy, then run live growth session, live single cycle, dry-run cycle, or preflight then live session |
| Unfollow | live cleanup-only unfollow, dry-run cleanup-only unfollow |
| Maintenance | reconcile live, reconcile dry-run, sync social graph cache |
| Diagnostics | probe reads, contract parity validation, contract validation with live reads |
| Observability | show latest run status, validate latest run artifact |
| Settings/Auth | edit defaults, run setup wizard, refresh auth session |

### Grouped Command Aliases

The original top-level commands are still supported. The new grouped aliases make the CLI structure match the start menu:

```bash
uv run bot growth session --tag programming
uv run bot growth session --policy warm-engage --source topic-recommended --tag programming
uv run bot growth cycle --dry-run --tag programming
uv run bot growth cycle --policy follow-only --source seed-followers --seed-user @username --dry-run --tag programming
uv run bot growth preflight --policy warm-engage-plus-rare-comment --source responders --tag programming
uv run bot growth followers --target-user @username --dry-run --single-cycle
uv run bot growth followers --target-user @username --live --session --target-follows 50 --scan-limit 100
uv run bot unfollow cleanup --live --limit 50
uv run bot maintenance sync --force
uv run bot maintenance reconcile --dry-run --limit 200 --page-size 50
uv run bot diagnostics probe --tag programming
uv run bot diagnostics contracts --tag programming --no-execute-reads
uv run bot observe status
uv run bot observe validate-artifact
```

Growth execution is now growth-only. Use `uv run bot cleanup ...` or `uv run bot unfollow cleanup ...` when you want cleanup/unfollow behavior.
If `--policy` / `--source` are omitted, growth commands fall back to `DEFAULT_GROWTH_POLICY` and `DEFAULT_GROWTH_SOURCES` from `.env`.
`uv run bot growth followers ...` remains the dedicated follow-only alias for harvesting candidates from a supplied user’s followers.

## Safety and Guardrails

- UTC day-boundary policy for all daily budgets.
- `MEDIUM_USER_REF` must be a Medium `user_id` (not `@username`).
- Safety behavior is operator-configurable in `.env`.
- Live session pacing supports a hard follow cap + soft follow floor envelope, and `LIVE_SESSION_MAX_PASSES` is enforced as a hard pass cap.
- Pacing soft-degrade can temporarily suspend mutations while keeping read paths active.
- Cleanup keeps high-follower accounts when `CLEANUP_UNFOLLOW_WHITELIST_MIN_FOLLOWERS` threshold is met.
- Cleanup treats missing follow timestamps as overdue by default (so legacy rows are not stuck pending forever).
- Cleanup candidate selection is constrained by local following cache membership to avoid stale unfollows.
- Cleanup unfollow pacing uses a dedicated short gap range, clamped to an effective `1-4s` window per action.
- Growth, unfollow, and reconcile flows can auto-sync cached social graph state before action execution using freshness-window controls.
- Live can halt on:
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
  - `USER_AGENT`
  - `CURL_IMPERSONATE`
  - `HTTP_ACCEPT_LANGUAGE`
  - `PLAYWRIGHT_AUTH_BROWSER_CHANNEL`
- budgets:
  - `MAX_ACTIONS_PER_DAY`
  - `DEFAULT_GROWTH_POLICY`
  - `DEFAULT_GROWTH_SOURCES`
  - `TARGET_USER_FOLLOWERS_SCAN_LIMIT`
  - `LIVE_SESSION_DURATION_MINUTES`
  - `LIVE_SESSION_TARGET_FOLLOW_ATTEMPTS`
  - `LIVE_SESSION_MIN_FOLLOW_ATTEMPTS`
  - `LIVE_SESSION_MAX_PASSES`
  - `MAX_FOLLOW_ACTIONS_PER_RUN`
  - `MAX_SUBSCRIBE_ACTIONS_PER_DAY`
  - `MAX_UNFOLLOW_ACTIONS_PER_DAY`
  - `MAX_CLAP_ACTIONS_PER_DAY`
  - `MAX_COMMENT_ACTIONS_PER_DAY`
- cleanup:
  - `UNFOLLOW_NONRECIPROCAL_AFTER_DAYS`
  - `CLEANUP_UNFOLLOW_LIMIT`
  - `CLEANUP_UNFOLLOW_WHITELIST_MIN_FOLLOWERS`
  - `CLEANUP_UNFOLLOW_MIN_GAP_SECONDS`
  - `CLEANUP_UNFOLLOW_MAX_GAP_SECONDS`
- graph sync:
  - `GRAPH_SYNC_AUTO_ENABLED`
  - `GRAPH_SYNC_FRESHNESS_WINDOW_MINUTES`
  - `GRAPH_SYNC_FULL_PAGINATION`
  - `GRAPH_SYNC_ENABLE_GRAPHQL_FOLLOWING`
  - `GRAPH_SYNC_ENABLE_SCRAPE_FALLBACK`
  - `GRAPH_SYNC_SCRAPE_PAGE_TIMEOUT_SECONDS`
- candidate filters:
  - `MIN_FOLLOWING_FOLLOWER_RATIO`
  - `MAX_FOLLOWING_FOLLOWER_RATIO`
  - `CANDIDATE_MIN_FOLLOWERS`
  - `CANDIDATE_MAX_FOLLOWERS`
  - `CANDIDATE_MIN_FOLLOWING`
  - `CANDIDATE_MAX_FOLLOWING`
  - `REQUIRE_CANDIDATE_BIO`
  - `REQUIRE_CANDIDATE_LATEST_POST`
  - `CANDIDATE_RECENT_ACTIVITY_DAYS`
  - `TOPIC_CURATED_LIST_ITEM_LIMIT`
  - `RESPONDER_POSTS_PER_RUN`
  - `RESPONDER_CANDIDATES_PER_POST`
- pacing:
  - `ENABLE_PRE_FOLLOW_CLAP`
  - `ENABLE_PRE_FOLLOW_COMMENT`
  - `PRE_FOLLOW_COMMENT_PROBABILITY`
  - `PRE_FOLLOW_COMMENT_TEMPLATES`
  - `PRE_FOLLOW_READ_WAIT_SECONDS`
  - `MIN_READ_WAIT_SECONDS`
  - `MIN_VERIFY_GAP_SECONDS`
  - `MAX_VERIFY_GAP_SECONDS`
  - `MIN_ACTION_GAP_SECONDS`
  - `MAX_ACTION_GAP_SECONDS`
  - `MAX_MUTATIONS_PER_10_MINUTES`
  - `PASS_COOLDOWN_MIN_SECONDS`
  - `PASS_COOLDOWN_MAX_SECONDS`
  - `PACING_SOFT_DEGRADE_COOLDOWN_SECONDS`
  - `ENABLE_PACING_AUTO_CLAMP`
- safety:
  - `RISK_HALT_CONSECUTIVE_FAILURES`
  - `RISK_HALT_MODE`
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
```

## References

- [Project-Overview.md](Project-Overview.md)
- [captures/README.md](captures/README.md)
