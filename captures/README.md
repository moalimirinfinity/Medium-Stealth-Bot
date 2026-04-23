# Captures

Implementation-ready GraphQL capture pack for Medium web flows.

## Runtime Use

The application uses these files for:

- operation-contract parity checks with `bot contracts`
- response-path validation
- runtime operation metadata lookup
- maintenance of the Medium integration contract layer

## Canonical Files

- `final/live_capture_2026-04-23.json`
  - authenticated request-level refresh with corrected comment probe and highlight create/delete probes
- `final/live_ops_2026-04-23.json`
  - compact operation summary derived from the April 23 live capture
- `final/implementation_ops_2026-04-23.json`
  - curated runtime-aligned operation subset with live-verified comment and highlight contracts
- `FOLLOW_ACTION_NOTE.md`
  - follow and unfollow semantics
- `IMPLEMENTATION_NOTES.md`
  - implementation guidance and integration rules
- `manifest.json`
  - canonical machine-readable index

## Evidence Levels

- `live_ui_observed`
  - recorded from real page/UI behavior
- `live_probe_stubbed`
  - mutation shape captured without applying real side effects

## Runtime Alignment

The current project model uses captures across distinct workflows:

- discovery reads and queue preparation
- growth execution and verification
- cleanup and rollback behavior
- diagnostics and contract checks

The capture registry is the shared contract layer across those workflows.

## Captured Flows (2026-04-23 Refresh)

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

For delete-comment coverage, set `MEDIUM_ACTIVITY_URL` in `.env` so the capture harness can visit that flow.
The harness now also includes stubbed probes for:

- `PublishPostThreadedResponse` using the current minimal `Delta` shape
- `QuoteCreateMutation`
- `DeleteQuoteMutation`

## CI Integrity Checks

Repository CI runs:

```bash
uv run python scripts/check_capture_integrity.py
uv run python scripts/check_capture_sanitization.py
```

These enforce:

- canonical manifest pointers resolve to real files
- canonical files are marked correctly in `manifest.json`
- capture freshness stays within threshold
- sanitized public-push invariants hold

## Public Sanitization Policy

Tracked files in `captures/final/*.json` must include:

- `sanitizedForPublicPush: true`

Sanitization removes or normalizes sensitive details such as:

- request-level `pageUrl`
- request body preview
- raw variable payload values
- request `referer`

## Refresh + Sanitize Flow

```bash
tmpdir=$(mktemp -d)
npm install --prefix "$tmpdir" playwright@latest
NODE_PATH="$tmpdir/node_modules" node captures/scripts/live_graphql_capture.js
uv run python scripts/sanitize_captures.py
uv run python scripts/check_capture_integrity.py
uv run python scripts/check_capture_sanitization.py
```
