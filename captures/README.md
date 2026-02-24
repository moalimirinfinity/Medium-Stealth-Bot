# Captures

Implementation-ready GraphQL capture pack for Medium web flows.

## Canonical Files
- `final/live_capture_2026-02-24.json`
  - Full request-level GraphQL capture collected with authenticated `.env` session cookies.
  - Includes request payloads, variables, request headers subset, response summaries, and `stubbed` marker.
- `final/live_ops_2026-02-24.json`
  - Compact operation summary derived from the live capture.
  - Includes operation list, mutation list, variable key sets, hit counts, and sample page URLs.
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

## CI Integrity Check

Repository CI runs:

```bash
uv run python scripts/check_capture_integrity.py
```

This enforces:
- canonical manifest pointers resolve to existing files
- canonical files are marked `isCanonical` in `manifest.json`
- capture freshness is within configured age threshold (`CAPTURE_MAX_AGE_DAYS`)
