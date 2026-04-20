# Captures

Implementation-ready GraphQL capture pack for Medium web flows.

## Runtime Use

The application uses these files for:

- operation-contract parity checks (`bot contracts`)
- response-path validation checks
- operation metadata lookup used during runtime request validation

## Canonical Files
- `final/live_capture_2026-02-24.json`
  - Full request-level GraphQL capture collected with authenticated `.env` session cookies.
  - Includes request payloads, variables, request headers subset, response summaries, and `stubbed` marker.
- `final/live_ops_2026-02-24.json`
  - Compact operation summary derived from the live capture.
  - Includes operation list, mutation list, variable key sets, hit counts, and sample page URLs.
- `final/live_capture_2026-04-20.json`
  - Targeted rollback-refresh capture focused on undo-clap and delete-comment payloads.
  - Confirms `DeleteResponseMutation` and negative `ClapMutation` request shapes with the current harness.
- `final/live_ops_2026-04-20.json`
  - Compact summary for the targeted rollback-refresh capture.
- `final/implementation_ops_2026-02-24.json`
  - Curated runtime-aligned subset of core operations needed for bot implementation (follow/unfollow/discovery/state checks + helper reads).
  - Includes operation-registry metadata: `classification`, `riskLevel`, variable contracts, and expected response fields.
- `FOLLOW_ACTION_NOTE.md`
  - Follow/unfollow semantics and how to classify state transitions for implementation.
- `IMPLEMENTATION_NOTES.md`
  - Direct implementation guidance (operation roles, safety, and integration rules).
- `manifest.json`
  - Machine-readable index and canonical capture pointers.

## Evidence Levels
- `live_ui_observed`
  - Captured from real page navigation and UI clicks with authenticated session.
- `live_probe_stubbed`
  - Side-effect mutations are intentionally fulfilled locally (stubbed) to avoid account changes.
  - Used only for payload/variable contract extraction.
- `legacy_reference`
  - Older historical captures retained for diffing (`2026-02-21`).

## Captured Flows (2026-02-24)
1. `https://medium.com/me/followers`
2. `https://medium.com/me/following`
3. `https://medium.com/tag/programming/latest`
4. `https://thilo-hermann.medium.com/the-day-we-forgot-about-layers-and-components-d6222451c4e2`

## Refresh Workflow
Run from repo root:

```bash
tmpdir=$(mktemp -d)
npm install --prefix "$tmpdir" playwright@latest
NODE_PATH="$tmpdir/node_modules" node captures/scripts/live_graphql_capture.js
```

This regenerates:
- `captures/final/live_capture_YYYY-MM-DD.json`
- `captures/final/live_ops_YYYY-MM-DD.json`

For delete-comment coverage, set `MEDIUM_ACTIVITY_URL` in `.env` to the signed-in account activity page before refreshing captures so the harness can visit that flow as well.

## CI Integrity Check

Repository CI runs:

```bash
uv run python scripts/check_capture_integrity.py
uv run python scripts/check_capture_sanitization.py
```

This enforces:
- canonical manifest pointers resolve to existing files
- canonical files are marked `isCanonical` in `manifest.json`
- capture freshness is within configured age threshold (`CAPTURE_MAX_AGE_DAYS`)
- sanitized public-push invariants are preserved

## Public Sanitization Policy

Tracked files in `captures/final/*.json` must include:

- `sanitizedForPublicPush: true`

Public sanitization removes high-granularity/sensitive capture fields:

- request-level `pageUrl`
- request-level `requestBodyPreview`
- raw `variables` payload values
- `referer` from request header subsets

URL-like fields are normalized to route-safe forms with no personal identifiers.

## Refresh + Sanitize Flow

```bash
tmpdir=$(mktemp -d)
npm install --prefix "$tmpdir" playwright@latest
NODE_PATH="$tmpdir/node_modules" node captures/scripts/live_graphql_capture.js
uv run python scripts/sanitize_captures.py
uv run python scripts/check_capture_integrity.py
uv run python scripts/check_capture_sanitization.py
```
