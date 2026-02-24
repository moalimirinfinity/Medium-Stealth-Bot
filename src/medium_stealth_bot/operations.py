from medium_stealth_bot.models import GraphQLOperation

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
      viewerEdge {
        id
        isFollowing
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
  }
}
""".strip()

PUBLISH_POST_THREADED_RESPONSE_MUTATION = """
mutation PublishPostThreadedResponse($inResponseToPostId: ID!, $deltas: [Delta!]!, $inResponseToQuoteId: ID, $responseDistribution: ResponseDistributionType, $sortType: ResponseSortType) {
  publishPostThreadedResponse(
    inResponseToPostId: $inResponseToPostId
    deltas: $deltas
    inResponseToQuoteId: $inResponseToQuoteId
    responseDistribution: $responseDistribution
    sortType: $sortType
  ) {
    __typename
    id
  }
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
    return GraphQLOperation(
        operationName="UserFollowers",
        query=USER_FOLLOWERS_QUERY,
        variables={
            "id": user_id,
            "username": username,
            "paging": {"limit": limit, "from": paging_from or ""},
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


def publish_threaded_response(
    in_response_to_post_id: str,
    text: str,
    response_distribution: str = "PUBLIC",
    sort_type: str = "NEWEST",
) -> GraphQLOperation:
    return GraphQLOperation(
        operationName="PublishPostThreadedResponse",
        query=PUBLISH_POST_THREADED_RESPONSE_MUTATION,
        variables={
            "inResponseToPostId": in_response_to_post_id,
            "deltas": [{"insert": text}],
            "inResponseToQuoteId": None,
            "responseDistribution": response_distribution,
            "sortType": sort_type,
        },
    )
