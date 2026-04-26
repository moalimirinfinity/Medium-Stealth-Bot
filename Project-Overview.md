# Project Overview: Medium Stealth Bot

## 1. Mission

Medium Stealth Bot is a local-first CLI for Medium growth automation with three core goals:

1. explicit, operator-controlled workflows
2. capture-backed Medium integration contracts
3. inspectable local state, queueing, and diagnostics

The project is designed for single-machine operation. Auth state, queue state, and artifacts stay local.

## 2. Architecture Snapshot

### Runtime stack

- Python 3.12+
- `uv`
- `Typer` + `Rich`
- `Pydantic v2` + `pydantic-settings`
- `structlog`
- `SQLite`
- `Playwright` and `curl-cffi`

### Client modes

1. `CLIENT_MODE=stealth`
   - Playwright persistent profile plus browser-backed request context
   - preferred for live runs
2. `CLIENT_MODE=fast`
   - `curl-cffi` session with impersonation support
   - lower overhead for some read-heavy workflows

### Auth bootstrap

1. `uv run bot auth`
2. sign in through the Playwright session
3. persist cookies/session fields into `.env`
4. reuse the local browser profile in `.data/playwright-profile`

## 3. Operational Model

The current project is separated into explicit pipelines.

### Discovery

Discovery reads Medium, scores and filters candidates, and persists only execution-ready queue rows. It is the candidate eligibility boundary and does not follow, clap, or comment. A live discovery run targets 100 newly eligible queue entries by default and stops earlier when the 700-row growth candidate queue cap is reached. Candidate ranking stores a score breakdown that combines bounded follow-back likelihood, layered topic affinity, source quality, newsletter availability, Medium presence, recent activity, negative-topic penalties, and conservative adaptive learning from completed follow cycles.

Current discovery sources:

- topic/recommended
- seed followers
- target-user followers
- publication adjacency
- responders

### Growth

Growth is queue-driven execution only. It drains execution-ready candidates already chosen by discovery and applies one of these policies:

- `follow-only`
- `warm-engage`
- `warm-engage-plus-rare-comment`

Growth does not perform discovery source selection anymore; legacy source flags remain only for compatibility.
Growth also does not re-score or re-filter queued candidates by candidate-quality rules such as ratio, follower/following bounds, bio, keywords, latest post, or recent activity. Those checks belong to discovery before enqueueing.
The growth queue is kept actionable and capped: non-actionable legacy rows are purged, total candidate rows are limited to 700 by default, and verified follows remove the candidate row.
The runtime rejects combined discovery+growth cycles; run discovery first, then run growth against the stored queue.

### Unfollow / cleanup

Cleanup is a separate maintenance workflow for overdue non-followback users.

### Maintenance

Maintenance includes:

- graph sync
- reconcile
- DB hygiene

### Diagnostics / observability

Diagnostics and observability cover:

- read probes
- contract validation
- queue counts
- status output
- artifact validation

## 4. Contract Source of Truth

Implementation contracts are derived from capture artifacts in `captures/final/`.
Canonical pointers are tracked in `captures/manifest.json`.

Primary files:

- `live_capture_2026-04-23.json`
- `live_ops_2026-04-23.json`
- `implementation_ops_2026-04-23.json`

The implementation registry defines:

- operation names and required variables
- classification labels such as `read`, `mutation`, `state-verify`, `high-risk`
- expected response paths used by validation

## 5. Medium Action Semantics

Observed Medium semantics used by the runtime:

1. subscribe path: `SubscribeNewsletterV3Mutation`
2. unsubscribe notifications path: `UnsubscribeNewsletterV3Mutation`
3. unfollow path: `UnfollowUserMutation`
4. canonical follow-state verification: `UserViewerEdge.user.viewerEdge.isFollowing`
5. clap rollback path: `ClapMutation` with negative `numClaps`
6. comment deletion path: `DeleteResponseMutation`
7. highlight create/delete path: `QuoteCreateMutation` and `DeleteQuoteMutation`

Design rule: newsletter subscription is not treated as definitive user-follow proof.

## 6. Local Data Model

### Core tables

- `users`
- `relationship_state`
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

### Growth queue model

Growth candidates are persisted with queue-oriented state and metadata such as:

- readiness
- deferred retry timing
- rejection / followed terminal state
- source labels
- score
- score breakdown JSON for auditability and learning
- freshness / reason fields

### Migration strategy

- numbered SQL migrations under `src/medium_stealth_bot/migrations/`
- history tracked in `schema_migrations`
- checksum validation guards against silent migration drift

## 7. Runtime Behavior

### Discovery (`bot discover`)

Pipeline stages:

1. optional graph sync
2. collect source candidates, including paginated follower pages for seed and target-user sources
3. score with persisted component breakdowns and filter
4. evaluate discovery-readiness
5. persist queue rows and reconciliation notes until the per-run eligible target or queue cap is reached
6. emit run artifact

Output: execution-ready queue entries only.

### Growth (`bot run`)

Pipeline stages:

1. optional graph sync
2. purge non-actionable legacy queue rows
3. fetch execution-ready queue candidates
4. apply growth policy
5. resolve post context only when needed for clap/comment/highlight action execution
6. re-check live follow state immediately before mutation
7. remove candidates after verified follows or already-following action guards
8. record attempts, verification, and queue state changes
9. emit run artifact

Output: follow/clap/comment execution against queued candidates.

### Reconcile (`bot reconcile`)

- builds a paginated worklist from reconciliation rows and pending follow-cycle users
- runs `UserViewerEdge`
- updates canonical follow-state in live mode

### Graph sync (`bot sync`)

- refreshes own followers and following cache tables
- upserts users from snapshots
- may import pending follow-cycle rows for known current following state

### Cleanup (`bot cleanup`)

- targets overdue non-followback rows
- uses cache-first filtering
- respects whitelist thresholds for high-follower accounts
- uses separate cleanup pacing

### DB hygiene (`bot db-hygiene`)

- prunes stale operational rows by retention windows
- supports dry-run preview and optional `VACUUM`

## 8. Safety and Timing

Hard-stop triggers can include:

- challenge detection
- session/auth expiry signatures
- repeated failure threshold
- `OPERATOR_KILL_SWITCH=true`

Timing model includes:

- session warmup
- read delay
- verify gap
- action gap
- mutation window limits
- pass cooldown
- cleanup-specific pacing

## 9. CLI Surface

Top-level commands:

- `setup`
- `start`
- `auth`
- `auth-import`
- `discover`
- `run`
- `queue`
- `cleanup`
- `sync`
- `reconcile`
- `db-hygiene`
- `probe`
- `contracts`
- `status`
- `artifacts validate`

Grouped aliases:

- `growth discover`
- `growth queue`
- `growth session`
- `growth cycle`
- `growth preflight` (hybrid: dry-run preflight, then live session)
- `growth followers`
- `unfollow cleanup`
- `maintenance sync`
- `maintenance reconcile`
- `maintenance db-hygiene`
- `diagnostics probe`
- `diagnostics contracts`
- `observe status`
- `observe queue`
- `observe validate-artifact`

### Start menu grouping

`uv run bot start` now groups options as:

- `1` discovery
- `2` growth
- `3` unfollow
- `4` maintenance
- `5` diagnostics
- `6` observability
- `7` settings/auth
- `8` exit

Within the growth menu, `Preflight` is a dry-run single pass and `Hybrid` is dry-run preflight followed by a live session. The grouped CLI alias `uv run bot growth preflight` currently maps to the hybrid behavior.

## 10. Operational Contracts

1. UTC day-boundary accounting is required for budgets.
2. `MEDIUM_USER_REF` must be a Medium `user_id`.
3. Discovery and growth are separate workflows.
4. Growth acts on the persisted queue; discovery populates it.
5. Follow verification is independent from newsletter subscribe semantics.

## 11. Quality and Validation

Current validation surface includes:

- contract parity checks
- response-path checks
- capture integrity checks
- capture sanitization checks
- compile/smoke checks
- artifact schema validation
- production profile validation

## 12. Current Maturity

Current project shape is stronger and more coherent than the earlier combined workflow:

- discovery, growth, unfollow, and maintenance are now explicit
- queue-driven growth is the default model
- observability and hygiene tooling are first-class operational features
- capture-driven contract validation remains the main Medium integration safety layer
