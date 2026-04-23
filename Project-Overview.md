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

Discovery reads Medium, scores/filter candidates, and persists execution-ready queue rows. It does not follow, clap, or comment.

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

- `live_capture_2026-02-24.json`
- `live_ops_2026-02-24.json`
- `implementation_ops_2026-02-24.json`
- `live_capture_2026-04-20.json` for rollback mutations

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
- freshness / reason fields

### Migration strategy

- numbered SQL migrations under `src/medium_stealth_bot/migrations/`
- history tracked in `schema_migrations`
- checksum validation guards against silent migration drift

## 7. Runtime Behavior

### Discovery (`bot discover`)

Pipeline stages:

1. optional graph sync
2. collect source candidates
3. score and filter
4. evaluate discovery-readiness
5. persist queue rows and reconciliation notes
6. emit run artifact

Output: execution-ready queue entries only.

### Growth (`bot run`)

Pipeline stages:

1. optional graph sync
2. fetch execution-ready queue candidates
3. apply growth policy
4. re-check follow state immediately before mutation
5. record attempts, verification, and queue state changes
6. emit run artifact

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
- `growth preflight`
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
