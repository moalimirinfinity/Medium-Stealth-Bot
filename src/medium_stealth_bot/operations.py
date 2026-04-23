from medium_stealth_bot.models import GraphQLOperation

USER_FOLLOWERS_MAX_LIMIT = 25
POST_RESPONSES_MAX_LIMIT = 25

USE_BASE_CACHE_CONTROL_QUERY = """
query UseBaseCacheControlQuery {
  viewer {
    __typename
    id
  }
}
""".strip()

TOPIC_LATEST_STORIE_QUERY = """
query TopicLatestStorieQuery($tagSlug: String!) {
  tagFromSlug(tagSlug: $tagSlug) {
    posts(timeRange: {kind: ALL_TIME}, sortOrder: NEWEST, first: 20) {
      edges {
        node {
          id
          title
          creator {
            id
            name
            username
            bio
            socialStats {
              followerCount
              followingCount
            }
            newsletterV3 {
              id
            }
          }
        }
      }
    }
  }
}
""".strip()

TOPIC_WHO_TO_FOLLOW_PUBLISHERS_QUERY = """
query TopicWhoToFollowPubishersQuery($first: Int!, $after: String!, $mode: RecommendedPublishersMode, $tagSlug: String) {
  recommendedPublishers(first: $first, after: $after, mode: $mode, tagSlug: $tagSlug) {
    edges {
      node {
        __typename
        ... on User {
          id
          name
          username
          bio
          socialStats {
            followerCount
            followingCount
          }
          newsletterV3 {
            id
          }
        }
        ... on Collection {
          id
          name
          slug
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
      startCursor
    }
  }
}
""".strip()

WHO_TO_FOLLOW_MODULE_QUERY = """
query WhoToFollowModuleQuery {
  recommendedPublishers(first: 3, after: "", mode: ALL) {
    edges {
      node {
        __typename
        ... on User {
          id
          name
          username
          bio
          socialStats {
            followerCount
            followingCount
          }
          newsletterV3 {
            id
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
      startCursor
    }
  }
}
""".strip()

USER_FOLLOWERS_QUERY = """
query UserFollowers($username: ID, $id: ID, $paging: PagingOptions) {
  userResult(username: $username, id: $id) {
    __typename
    ... on User {
      id
      followersUserConnection(paging: $paging) {
        users {
          id
          name
          username
          bio
          socialStats {
            followerCount
            followingCount
          }
          newsletterV3 {
            id
          }
        }
        pagingInfo {
          next {
            from
            limit
          }
        }
      }
    }
  }
}
""".strip()

USER_LATEST_POST_QUERY = """
query UserLatestPostQuery($id: ID, $username: ID) {
  userResult(id: $id, username: $username) {
    __typename
    ... on User {
      id
      homepagePostsConnection(paging: {limit: 1}) {
        posts {
          id
          title
          creator {
            id
          }
        }
      }
    }
  }
}
""".strip()

USER_VIEWER_EDGE_QUERY = """
query UserViewerEdge($userId: ID!) {
  user(id: $userId) {
    ... on User {
      id
      name
      username
      bio
      newsletterV3 {
        id
      }
      socialStats {
        followerCount
        followingCount
      }
      viewerEdge {
        id
        isFollowing
        lastPostCreatedAt
      }
    }
  }
}
""".strip()

TOPIC_CURATED_LIST_QUERY = """
query TopicCuratedListQuery($tagSlug: String!, $itemLimit: Int!) {
  tagFromSlug(tagSlug: $tagSlug) {
    curatedLists(first: 1) {
      edges {
        node {
          id
          name
          itemsConnection(pagingOptions: {limit: $itemLimit}) {
            items {
              catalogItemId
              entity {
                ... on Post {
                  id
                  title
                  creator {
                    id
                    name
                    username
                    bio
                    socialStats {
                      followerCount
                      followingCount
                    }
                    newsletterV3 {
                      id
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
""".strip()

POST_RESPONSES_QUERY = """
query PostResponsesQuery($postId: ID!, $paging: PagingOptions, $sortType: ResponseSortType) {
  post(id: $postId) {
    id
    threadedPostResponses(paging: $paging, sortType: $sortType) {
      posts {
        id
        creator {
          id
          name
          username
          bio
          newsletterV3 {
            id
          }
          socialStats {
            followerCount
            followingCount
          }
        }
      }
      pagingInfo {
        next {
          limit
          to
        }
      }
    }
  }
}
""".strip()

NEWSLETTER_V3_VIEWER_EDGE_QUERY = """
query NewsletterV3ViewerEdge($newsletterSlug: ID!, $collectionSlug: ID, $username: ID) {
  newsletterV3(newsletterSlug: $newsletterSlug, collectionSlug: $collectionSlug, username: $username) {
    ... on NewsletterV3 {
      id
      viewerEdge {
        id
        isSubscribed
      }
    }
  }
}
""".strip()

SUBSCRIBE_NEWSLETTER_V3_MUTATION = """
mutation SubscribeNewsletterV3Mutation($newsletterV3Id: ID!, $shouldRecordConsent: Boolean) {
  subscribeNewsletterV3(newsletterV3Id: $newsletterV3Id, shouldRecordConsent: $shouldRecordConsent)
}
""".strip()

UNSUBSCRIBE_NEWSLETTER_V3_MUTATION = """
mutation UnsubscribeNewsletterV3Mutation($newsletterV3Id: ID!) {
  unsubscribeNewsletterV3(newsletterV3Id: $newsletterV3Id)
}
""".strip()

UNFOLLOW_USER_MUTATION = """
mutation UnfollowUserMutation($targetUserId: ID!) {
  unfollowUser(targetUserId: $targetUserId) {
    __typename
    id
    name
    viewerEdge {
      __typename
      id
      isFollowing
    }
  }
}
""".strip()

CLAP_MUTATION = """
mutation ClapMutation($targetPostId: ID!, $userId: ID!, $numClaps: Int!) {
  clap(targetPostId: $targetPostId, userId: $userId, numClaps: $numClaps) {
    __typename
    id
    clapCount
    viewerEdge {
      __typename
      id
      clapCount
    }
  }
}
""".strip()

PUBLISH_POST_THREADED_RESPONSE_MUTATION = """
mutation PublishPostThreadedResponse($inResponseToPostId: ID!, $deltas: [Delta!]!, $inResponseToQuoteId: ID) {
  publishPostThreadedResponse(
    inResponseToPostId: $inResponseToPostId
    deltas: $deltas
    inResponseToQuoteId: $inResponseToQuoteId
  ) {
    __typename
    id
  }
}
""".strip()

DELETE_RESPONSE_MUTATION = """
mutation DeleteResponseMutation($responseId: ID!) {
  deletePost(targetPostId: $responseId)
}
""".strip()

QUOTE_CREATE_MUTATION = """
mutation QuoteCreateMutation($targetPostId: ID!, $targetPostVersionId: ID!, $targetParagraphNames: [ID!]!, $startOffset: Int!, $endOffset: Int!, $quoteType: StreamItemQuoteType!) {
  createQuote(
    targetPostId: $targetPostId
    targetPostVersionId: $targetPostVersionId
    targetParagraphNames: $targetParagraphNames
    startOffset: $startOffset
    endOffset: $endOffset
    quoteType: $quoteType
  ) {
    __typename
    id
  }
}
""".strip()

DELETE_QUOTE_MUTATION = """
mutation DeleteQuoteMutation($targetPostId: ID!, $targetQuoteId: ID!) {
  deleteQuote(targetPostId: $targetPostId, targetQuoteId: $targetQuoteId)
}
""".strip()


def use_base_cache_control() -> GraphQLOperation:
    return GraphQLOperation(
        operationName="UseBaseCacheControlQuery",
        query=USE_BASE_CACHE_CONTROL_QUERY,
        variables={},
    )


def topic_latest_stories(tag_slug: str) -> GraphQLOperation:
    return GraphQLOperation(
        operationName="TopicLatestStorieQuery",
        query=TOPIC_LATEST_STORIE_QUERY,
        variables={"tagSlug": tag_slug},
    )


def topic_who_to_follow_publishers(
    tag_slug: str,
    first: int = 3,
    after: str = "",
    mode: str = "ALL",
) -> GraphQLOperation:
    return GraphQLOperation(
        operationName="TopicWhoToFollowPubishersQuery",
        query=TOPIC_WHO_TO_FOLLOW_PUBLISHERS_QUERY,
        variables={"first": first, "after": after, "mode": mode, "tagSlug": tag_slug},
    )


def who_to_follow_module() -> GraphQLOperation:
    return GraphQLOperation(
        operationName="WhoToFollowModuleQuery",
        query=WHO_TO_FOLLOW_MODULE_QUERY,
        variables={},
    )


def user_followers(
    *,
    user_id: str | None = None,
    username: str | None = None,
    limit: int = 8,
    paging_from: str | None = None,
) -> GraphQLOperation:
    resolved_limit = max(1, min(USER_FOLLOWERS_MAX_LIMIT, int(limit)))
    return GraphQLOperation(
        operationName="UserFollowers",
        query=USER_FOLLOWERS_QUERY,
        variables={
            "id": user_id,
            "username": username,
            "paging": {"limit": resolved_limit, "from": paging_from or ""},
        },
    )


def user_latest_post(*, user_id: str | None = None, username: str | None = None) -> GraphQLOperation:
    return GraphQLOperation(
        operationName="UserLatestPostQuery",
        query=USER_LATEST_POST_QUERY,
        variables={"id": user_id, "username": username},
    )


def user_viewer_edge(user_id: str) -> GraphQLOperation:
    return GraphQLOperation(
        operationName="UserViewerEdge",
        query=USER_VIEWER_EDGE_QUERY,
        variables={"userId": user_id},
    )


def topic_curated_list(tag_slug: str, *, item_limit: int = 6) -> GraphQLOperation:
    return GraphQLOperation(
        operationName="TopicCuratedListQuery",
        query=TOPIC_CURATED_LIST_QUERY,
        variables={"tagSlug": tag_slug, "itemLimit": max(1, min(25, int(item_limit)))},
    )


def post_responses(
    *,
    post_id: str,
    limit: int = 10,
    paging_to: str | None = None,
    sort_type: str = "NEWEST",
) -> GraphQLOperation:
    resolved_limit = max(1, min(POST_RESPONSES_MAX_LIMIT, int(limit)))
    return GraphQLOperation(
        operationName="PostResponsesQuery",
        query=POST_RESPONSES_QUERY,
        variables={
            "postId": post_id,
            "paging": {
                "limit": resolved_limit,
                "to": paging_to or "",
            },
            "sortType": sort_type,
        },
    )


def newsletter_v3_viewer_edge(
    newsletter_slug: str,
    username: str | None = None,
    collection_slug: str | None = None,
) -> GraphQLOperation:
    return GraphQLOperation(
        operationName="NewsletterV3ViewerEdge",
        query=NEWSLETTER_V3_VIEWER_EDGE_QUERY,
        variables={
            "newsletterSlug": newsletter_slug,
            "collectionSlug": collection_slug,
            "username": username,
        },
    )


def subscribe_newsletter_v3(newsletter_v3_id: str, should_record_consent: bool = False) -> GraphQLOperation:
    return GraphQLOperation(
        operationName="SubscribeNewsletterV3Mutation",
        query=SUBSCRIBE_NEWSLETTER_V3_MUTATION,
        variables={
            "newsletterV3Id": newsletter_v3_id,
            "shouldRecordConsent": should_record_consent,
        },
    )


def unsubscribe_newsletter_v3(newsletter_v3_id: str) -> GraphQLOperation:
    return GraphQLOperation(
        operationName="UnsubscribeNewsletterV3Mutation",
        query=UNSUBSCRIBE_NEWSLETTER_V3_MUTATION,
        variables={"newsletterV3Id": newsletter_v3_id},
    )


def unfollow_user(target_user_id: str) -> GraphQLOperation:
    return GraphQLOperation(
        operationName="UnfollowUserMutation",
        query=UNFOLLOW_USER_MUTATION,
        variables={"targetUserId": target_user_id},
    )


def clap_post(target_post_id: str, user_id: str, num_claps: int = 1) -> GraphQLOperation:
    return GraphQLOperation(
        operationName="ClapMutation",
        query=CLAP_MUTATION,
        variables={
            "targetPostId": target_post_id,
            "userId": user_id,
            "numClaps": num_claps,
        },
    )


def undo_clap_post(target_post_id: str, user_id: str, num_claps: int) -> GraphQLOperation:
    return clap_post(target_post_id, user_id, num_claps=-abs(num_claps))


def _threaded_response_deltas(text: str) -> list[dict[str, object]]:
    # Live-validated on 2026-04-23: Medium now accepts a minimal Delta payload
    # for responses. The older {"insert": "..."} shape is rejected.
    return [
        {
            "type": 1,
            "index": 0,
            "paragraph": {
                "name": "p000",
                "type": 1,
                "text": text,
                "markups": [],
            },
        }
    ]


def publish_threaded_response(
    in_response_to_post_id: str,
    text: str,
) -> GraphQLOperation:
    return GraphQLOperation(
        operationName="PublishPostThreadedResponse",
        query=PUBLISH_POST_THREADED_RESPONSE_MUTATION,
        variables={
            "inResponseToPostId": in_response_to_post_id,
            "deltas": _threaded_response_deltas(text),
            "inResponseToQuoteId": None,
        },
    )


def delete_response(response_id: str) -> GraphQLOperation:
    return GraphQLOperation(
        operationName="DeleteResponseMutation",
        query=DELETE_RESPONSE_MUTATION,
        variables={"responseId": response_id},
    )


def create_quote_highlight(
    *,
    target_post_id: str,
    target_post_version_id: str,
    target_paragraph_names: list[str],
    start_offset: int,
    end_offset: int,
    quote_type: str = "HIGHLIGHT",
) -> GraphQLOperation:
    return GraphQLOperation(
        operationName="QuoteCreateMutation",
        query=QUOTE_CREATE_MUTATION,
        variables={
            "targetPostId": target_post_id,
            "targetPostVersionId": target_post_version_id,
            "targetParagraphNames": target_paragraph_names,
            "startOffset": start_offset,
            "endOffset": end_offset,
            "quoteType": quote_type,
        },
    )


def delete_quote(*, target_post_id: str, target_quote_id: str) -> GraphQLOperation:
    return GraphQLOperation(
        operationName="DeleteQuoteMutation",
        query=DELETE_QUOTE_MUTATION,
        variables={
            "targetPostId": target_post_id,
            "targetQuoteId": target_quote_id,
        },
    )
