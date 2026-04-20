from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from medium_stealth_bot.models import GraphQLResult


class _Model(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class SocialStats(_Model):
    follower_count: int | None = Field(default=None, alias="followerCount")
    following_count: int | None = Field(default=None, alias="followingCount")


class NewsletterV3(_Model):
    id: str | None = None


class UserNode(_Model):
    id: str
    username: str | None = None
    name: str | None = None
    bio: str | None = None
    social_stats: SocialStats | None = Field(default=None, alias="socialStats")
    newsletter_v3: NewsletterV3 | None = Field(default=None, alias="newsletterV3")


class TagPostNode(_Model):
    id: str | None = None
    creator: UserNode | None = None


class TagPostEdge(_Model):
    node: TagPostNode | None = None


class TagPostsConnection(_Model):
    edges: list[TagPostEdge] = Field(default_factory=list)


class TagFromSlug(_Model):
    posts: TagPostsConnection | None = None


class TopicLatestStoriesData(_Model):
    tag_from_slug: TagFromSlug | None = Field(default=None, alias="tagFromSlug")


class RecommendedNode(_Model):
    typename: str | None = Field(default=None, alias="__typename")
    id: str | None = None
    username: str | None = None
    name: str | None = None
    bio: str | None = None
    social_stats: SocialStats | None = Field(default=None, alias="socialStats")
    newsletter_v3: NewsletterV3 | None = Field(default=None, alias="newsletterV3")


class RecommendedEdge(_Model):
    node: RecommendedNode | None = None


class RecommendedPublishers(_Model):
    edges: list[RecommendedEdge] = Field(default_factory=list)


class RecommendedPublishersData(_Model):
    recommended_publishers: RecommendedPublishers | None = Field(default=None, alias="recommendedPublishers")


class FollowersUserConnection(_Model):
    users: list[UserNode] = Field(default_factory=list)
    paging_info: "PagingInfo | None" = Field(default=None, alias="pagingInfo")


class PagingCursor(_Model):
    from_cursor: str | None = Field(default=None, alias="from")
    limit: int | None = None


class PagingInfo(_Model):
    next: PagingCursor | None = None


class UserResultFollowers(_Model):
    followers_user_connection: FollowersUserConnection | None = Field(default=None, alias="followersUserConnection")


class UserFollowersData(_Model):
    user_result: UserResultFollowers | None = Field(default=None, alias="userResult")


class HomepagePostsConnection(_Model):
    posts: list[TagPostNode] = Field(default_factory=list)


class UserResultLatestPost(_Model):
    homepage_posts_connection: HomepagePostsConnection | None = Field(default=None, alias="homepagePostsConnection")


class UserLatestPostData(_Model):
    user_result: UserResultLatestPost | None = Field(default=None, alias="userResult")


class ViewerEdge(_Model):
    is_following: bool | None = Field(default=None, alias="isFollowing")


class UserForViewerEdge(_Model):
    username: str | None = None
    social_stats: SocialStats | None = Field(default=None, alias="socialStats")
    viewer_edge: ViewerEdge | None = Field(default=None, alias="viewerEdge")


class UserViewerEdgeData(_Model):
    user: UserForViewerEdge | None = None


class NewsletterViewerEdge(_Model):
    is_subscribed: bool | None = Field(default=None, alias="isSubscribed")


class NewsletterV3Viewer(_Model):
    viewer_edge: NewsletterViewerEdge | None = Field(default=None, alias="viewerEdge")


class NewsletterV3ViewerEdgeData(_Model):
    newsletter_v3: NewsletterV3Viewer | None = Field(default=None, alias="newsletterV3")


class ClapViewerEdge(_Model):
    clap_count: int | None = Field(default=None, alias="clapCount")


class ClapPayload(_Model):
    clap_count: int | None = Field(default=None, alias="clapCount")
    viewer_edge: ClapViewerEdge | None = Field(default=None, alias="viewerEdge")


class ClapMutationData(_Model):
    clap: ClapPayload | None = None


class PublishPostThreadedResponsePayload(_Model):
    id: str | None = None


class PublishPostThreadedResponseData(_Model):
    publish_post_threaded_response: PublishPostThreadedResponsePayload | None = Field(
        default=None,
        alias="publishPostThreadedResponse",
    )


class DeleteResponseMutationData(_Model):
    delete_post: bool | None = Field(default=None, alias="deletePost")


def parse_topic_latest_story_creators(result: GraphQLResult) -> list[tuple[UserNode, str | None]]:
    payload = TopicLatestStoriesData.model_validate(result.data or {})
    creators: list[tuple[UserNode, str | None]] = []
    edges = payload.tag_from_slug.posts.edges if payload.tag_from_slug and payload.tag_from_slug.posts else []
    for edge in edges:
        if not edge.node or not edge.node.creator:
            continue
        creators.append((edge.node.creator, edge.node.id))
    return creators


def parse_recommended_publishers_users(result: GraphQLResult) -> list[UserNode]:
    payload = RecommendedPublishersData.model_validate(result.data or {})
    users: list[UserNode] = []
    edges = payload.recommended_publishers.edges if payload.recommended_publishers else []
    for edge in edges:
        if edge.node is None:
            continue
        if edge.node.typename and edge.node.typename != "User":
            continue
        if not edge.node.id:
            continue
        users.append(UserNode.model_validate(edge.node.model_dump(by_alias=True)))
    return users


def parse_user_followers_users(result: GraphQLResult) -> list[UserNode]:
    payload = UserFollowersData.model_validate(result.data or {})
    if not payload.user_result or not payload.user_result.followers_user_connection:
        return []
    return payload.user_result.followers_user_connection.users


def parse_user_followers_next_from(result: GraphQLResult) -> str | None:
    payload = UserFollowersData.model_validate(result.data or {})
    if not payload.user_result or not payload.user_result.followers_user_connection:
        return None
    paging = payload.user_result.followers_user_connection.paging_info
    if not paging or not paging.next:
        return None
    value = paging.next.from_cursor
    if value is None:
        return None
    text = value.strip()
    return text or None


def parse_latest_post_id(result: GraphQLResult) -> str | None:
    payload = UserLatestPostData.model_validate(result.data or {})
    if not payload.user_result or not payload.user_result.homepage_posts_connection:
        return None
    posts = payload.user_result.homepage_posts_connection.posts
    if not posts:
        return None
    return posts[0].id


def parse_user_viewer_is_following(result: GraphQLResult) -> bool | None:
    payload = UserViewerEdgeData.model_validate(result.data or {})
    if not payload.user or not payload.user.viewer_edge:
        return None
    return payload.user.viewer_edge.is_following


def parse_user_viewer_follower_count(result: GraphQLResult) -> int | None:
    payload = UserViewerEdgeData.model_validate(result.data or {})
    if not payload.user or not payload.user.social_stats:
        return None
    return payload.user.social_stats.follower_count


def parse_newsletter_is_subscribed(result: GraphQLResult) -> bool | None:
    payload = NewsletterV3ViewerEdgeData.model_validate(result.data or {})
    if not payload.newsletter_v3 or not payload.newsletter_v3.viewer_edge:
        return None
    return payload.newsletter_v3.viewer_edge.is_subscribed


def parse_clap_count(result: GraphQLResult) -> int | None:
    payload = ClapMutationData.model_validate(result.data or {})
    if not payload.clap:
        return None
    return payload.clap.clap_count


def parse_viewer_clap_count(result: GraphQLResult) -> int | None:
    payload = ClapMutationData.model_validate(result.data or {})
    if not payload.clap or not payload.clap.viewer_edge:
        return None
    return payload.clap.viewer_edge.clap_count


def parse_publish_threaded_response_id(result: GraphQLResult) -> str | None:
    payload = PublishPostThreadedResponseData.model_validate(result.data or {})
    if not payload.publish_post_threaded_response:
        return None
    return payload.publish_post_threaded_response.id


def parse_delete_response_success(result: GraphQLResult) -> bool | None:
    payload = DeleteResponseMutationData.model_validate(result.data or {})
    return payload.delete_post
