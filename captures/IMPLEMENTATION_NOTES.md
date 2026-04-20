# Implementation Notes

## Canonical Capture
Use `captures/final/live_capture_2026-02-24.json` as primary truth for implementation.
Use `captures/final/live_capture_2026-04-20.json` as targeted supplemental evidence for rollback mutations (`DeleteResponseMutation` and negative `ClapMutation`).
Use `captures/final/implementation_ops_2026-02-24.json` as the focused operation subset for coding.
The focused subset is runtime-aligned: capture-observed operations plus the runtime helper `UserLatestPostQuery`.
The subset is also a machine-readable operation registry with per-operation `classification`, `riskLevel`,
`requiredVariableKeys`, `optionalVariableKeys`, and `expectedTopLevelResponseFields`.

## Endpoint
- GraphQL endpoint: `https://medium.com/_/graphql`
- Payload transport: JSON array of operation objects.

Single request shape:
```json
[
  {
    "operationName": "UserViewerEdge",
    "variables": {
      "userId": "..."
    },
    "query": "query UserViewerEdge(...) { ... }"
  }
]
```

## Critical Operation Contracts

### Follow UI Path
- Operation: `SubscribeNewsletterV3Mutation`
- Variables:
  - `newsletterV3Id`
  - `shouldRecordConsent` (observed `false`)
- Classification: `newsletter_subscribe` (not definitive `user_follow`).

### Unsubscribe Notifications
- Operation: `UnsubscribeNewsletterV3Mutation`
- Variables:
  - `newsletterV3Id`
- Classification: `newsletter_unsubscribe`.

### Full Unfollow
- Operation: `UnfollowUserMutation`
- Variables:
  - `targetUserId`
- Classification: `user_unfollow`.

### Undo Claps
- Operation: `ClapMutation`
- Variables:
  - `targetPostId`
  - `userId`
  - `numClaps`
- Rule: rollback uses the same mutation with a negative `numClaps` value.

### Delete Comment / Response
- Operation: `DeleteResponseMutation`
- Variables:
  - `responseId`
- Backend field:
  - `deletePost`
- Note: current evidence comes from the account activity flow (`/activity`) rather than the older capture bundle.

### Verify Actual Follow State
- Operation: `UserViewerEdge`
- Signal:
  - `user.viewerEdge.isFollowing`
- Rule: count `user_follow` only when `isFollowing == true`.

### Discover Targets
- Operations:
  - `UseBaseCacheControlQuery`
  - `TopicLatestStorieQuery`
  - `TopicWhoToFollowPubishersQuery`
  - `WhoToFollowModuleQuery`
  - `UserLatestPostQuery` (runtime helper for optional pre-follow clap target resolution)
  - `NewsletterV3ViewerEdge`

## Safety Notes
- In `live_capture_2026-02-24.json`, side-effect mutation probes are marked with `"stubbed": true`.
- Stubbed records are safe for payload contract extraction, but not proof of backend success semantics.
- Treat non-stubbed UI-observed records as behavior evidence.

## Practical Build Rule Set
1. Separate states: `newsletter_subscribe`, `newsletter_unsubscribe`, `user_follow`, `user_unfollow`.
2. Never infer `user_follow` from newsletter subscription alone.
3. Verify follow state with `UserViewerEdge` after follow/unfollow decisions.
4. Persist both `user_id` and `newsletter_v3_id` in storage.
5. Validate outgoing operations against the registry contract before request execution.

## Operational Implications

- Reconcile accuracy depends on `UserViewerEdge` reliability.
- Cleanup accuracy depends on separating subscription-state signals from true graph follow-state signals.
- Graph sync and cache refresh should not infer user-follow state without explicit verify reads.
- Public-engagement cleanup should undo claps with negative `ClapMutation` payloads and remove bot-authored comments with `DeleteResponseMutation`.

## Classification Taxonomy
- `read`: no intended state change.
- `mutation`: state-changing operation.
- `state-verify`: canonical verification read (`UserViewerEdge`, `NewsletterV3ViewerEdge`).
- `high-risk`: mutation with account-visible side effects.
