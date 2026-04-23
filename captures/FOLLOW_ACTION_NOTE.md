# Follow Action Note

## Current Mapping (Validated 2026-04-23)

### Default Follow Button
- Operation: `SubscribeNewsletterV3Mutation`
- Variables:
  - `newsletterV3Id`
  - `shouldRecordConsent: false`
- Meaning: newsletter subscription path, not guaranteed full graph follow.

### Email Notifications Off
- Operation: `UnsubscribeNewsletterV3Mutation`
- Variables:
  - `newsletterV3Id`
- Meaning: newsletter subscription preference change.

### Full Unfollow
- Operation: `UnfollowUserMutation`
- Variables:
  - `targetUserId`
- Meaning: definitive user graph unfollow path.

## Why This Matters
- `newsletter_subscribe` and `user_follow` are different states.
- If you track only newsletter state, growth metrics will drift from true follow graph state.

## Evidence Sources
- Live capture:
  - `captures/final/live_capture_2026-04-23.json`
  - `captures/final/live_ops_2026-04-23.json`
  - `captures/final/implementation_ops_2026-04-23.json`

## Evidence Confidence
- `SubscribeNewsletterV3Mutation`: live UI-observed + payload captured.
- `UnsubscribeNewsletterV3Mutation`: payload probe captured with `stubbed: true`.
- `UnfollowUserMutation`: payload probe captured with `stubbed: true`.

## Implementation Rules
1. Store and report these states separately:
   - `newsletter_subscribe`
   - `newsletter_unsubscribe`
   - `user_follow`
   - `user_unfollow`
2. Count `user_follow` only when `UserViewerEdge.user.viewerEdge.isFollowing == true`.
3. Never treat newsletter unsubscribe as full user unfollow.

## Runtime Alignment

- Reconcile and cleanup workflows should always read canonical `user_follow` state from verification operations or trusted cache+verification flow.
- Reporting should keep newsletter and user-follow metrics separate to avoid false growth/unfollow signals.
