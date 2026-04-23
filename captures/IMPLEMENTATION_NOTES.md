# Implementation Notes

## Canonical Capture Set

Use `captures/manifest.json` as the canonical pointer source.

Current primary references:

- `captures/final/live_capture_2026-04-23.json`
- `captures/final/live_ops_2026-04-23.json`
- `captures/final/implementation_ops_2026-04-23.json`

The implementation subset is the runtime-aligned operation registry. It includes:

- `classification`
- `riskLevel`
- required and optional variable keys
- expected response fields

## Endpoint

- GraphQL endpoint: `https://medium.com/_/graphql`
- transport shape: JSON array of operation objects

Example request:

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

## Runtime-Aligned Operation Roles

### Growth execution

- `SubscribeNewsletterV3Mutation`
- `UnsubscribeNewsletterV3Mutation`
- `UnfollowUserMutation`
- `ClapMutation`
- `DeleteResponseMutation`
- `QuoteCreateMutation`
- `DeleteQuoteMutation`
- `UserViewerEdge`

### Discovery and queue preparation

- `UseBaseCacheControlQuery`
- `TopicLatestStorieQuery`
- `TopicWhoToFollowPubishersQuery`
- `WhoToFollowModuleQuery`
- `UserLatestPostQuery`
- `NewsletterV3ViewerEdge`

Discovery now fills the queue only. Growth execution consumes already-selected queue candidates.

## Critical Semantics

### Follow UI path

- operation: `SubscribeNewsletterV3Mutation`
- required variable:
  - `newsletterV3Id`
- note:
  - newsletter subscribe is not definitive proof of graph follow state

### Full unfollow

- operation: `UnfollowUserMutation`
- variable:
  - `targetUserId`

### Verify actual follow state

- operation: `UserViewerEdge`
- canonical signal:
  - `user.viewerEdge.isFollowing`

### Undo clap

- operation: `ClapMutation`
- variables:
  - `targetPostId`
  - `userId`
  - `numClaps`
- rule:
  - rollback uses a negative `numClaps`
- live verification:
  - confirmed on 2026-04-23 via Playwright-backed authenticated session with `numClaps=-1`

### Delete comment / response

- operation: `DeleteResponseMutation`
- variable:
  - `responseId`
- backend field:
  - `deletePost`
- live verification:
  - confirmed on 2026-04-23 via Playwright-backed authenticated session

### Publish comment / response

- operation: `PublishPostThreadedResponse`
- required variables:
  - `inResponseToPostId`
  - `deltas`
- optional variables:
  - `inResponseToQuoteId`
- live-validated minimal payload:
  - a single paragraph delta with `type=1`, `index=0`, and `paragraph={name,type,text,markups}`
- important drift:
  - the legacy `{"insert": "..."}` delta shape is rejected
  - `responseDistribution` is optional for the minimal mutation
  - `sortType` should not be declared unless the query actually uses it

### Create highlight

- operation: `QuoteCreateMutation`
- required variables:
  - `targetPostId`
  - `targetPostVersionId`
  - `targetParagraphNames`
  - `startOffset`
  - `endOffset`
  - `quoteType`
- live verification:
  - confirmed on 2026-04-23 via Playwright-backed authenticated session
- observed response field:
  - `createQuote.id`

### Delete highlight / quote

- operation: `DeleteQuoteMutation`
- required variables:
  - `targetPostId`
  - `targetQuoteId`
- backend field:
  - `deleteQuote`
- live verification:
  - confirmed on 2026-04-23 via Playwright-backed authenticated session

## Safety Notes

- Some live-capture mutation probes are marked `stubbed`.
- Stubbed records are valid for payload and variable contract extraction, not for backend success semantics.
- Prefer non-stubbed UI-observed records when reasoning about actual behavior.

## Practical Build Rules

1. Keep `newsletter_subscribe`, `newsletter_unsubscribe`, `user_follow`, and `user_unfollow` as separate concepts.
2. Never infer true follow state from newsletter subscribe alone.
3. Verify follow state with `UserViewerEdge`.
4. Persist both `user_id` and `newsletter_v3_id` when available.
5. Validate outgoing operations against the registry before execution.
6. Keep discovery-side reads and growth-side mutations as separate workflows.

## Operational Implications

- Reconcile depends on `UserViewerEdge` reliability.
- Cleanup depends on separating subscription semantics from graph follow semantics.
- Graph sync should not infer user-follow state without explicit verification.
- Rollback flows depend on negative clap support, `DeleteResponseMutation`, and `DeleteQuoteMutation`.

## Classification Taxonomy

- `read`: no intended state change
- `mutation`: state-changing operation
- `state-verify`: canonical verification read
- `high-risk`: account-visible mutation
