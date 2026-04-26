from __future__ import annotations

from typing import Any

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


class PostMarkup(_Model):
    type: str | None = None
    start: int | None = None
    end: int | None = None


class PostParagraph(_Model):
    name: str | None = None
    text: str | None = None
    type: str | int | None = None
    markups: list[PostMarkup] = Field(default_factory=list)


class PostBodyModel(_Model):
    paragraphs: list[PostParagraph] = Field(default_factory=list)


class PostContent(_Model):
    body_model: PostBodyModel | None = Field(default=None, alias="bodyModel")


class PostVersion(_Model):
    id: str | None = None


class TagPostNode(_Model):
    id: str | None = None
    title: str | None = None
    latest_published_version_id: str | None = Field(default=None, alias="latestPublishedVersionId")
    latest_published_version: str | PostVersion | None = Field(default=None, alias="latestPublishedVersion")
    content: PostContent | None = None
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
    last_post_created_at: Any = Field(default=None, alias="lastPostCreatedAt")


class UserForViewerEdge(_Model):
    id: str | None = None
    name: str | None = None
    username: str | None = None
    bio: str | None = None
    social_stats: SocialStats | None = Field(default=None, alias="socialStats")
    newsletter_v3: NewsletterV3 | None = Field(default=None, alias="newsletterV3")
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


class QuoteCreatePayload(_Model):
    id: str | None = None


class QuoteCreateMutationData(_Model):
    create_quote: QuoteCreatePayload | None = Field(default=None, alias="createQuote")


class DeleteQuoteMutationData(_Model):
    delete_quote: bool | None = Field(default=None, alias="deleteQuote")


class CatalogItemEntityPost(_Model):
    id: str | None = None
    title: str | None = None
    creator: UserNode | None = None


class TopicCuratedItem(_Model):
    entity: CatalogItemEntityPost | None = None


class TopicCuratedItemsConnection(_Model):
    items: list[TopicCuratedItem] = Field(default_factory=list)


class TopicCuratedListNode(_Model):
    items_connection: TopicCuratedItemsConnection | None = Field(default=None, alias="itemsConnection")


class TopicCuratedListEdge(_Model):
    node: TopicCuratedListNode | None = None


class TopicCuratedLists(_Model):
    edges: list[TopicCuratedListEdge] = Field(default_factory=list)


class TopicCuratedListTag(_Model):
    curated_lists: TopicCuratedLists | None = Field(default=None, alias="curatedLists")


class TopicCuratedListData(_Model):
    tag_from_slug: TopicCuratedListTag | None = Field(default=None, alias="tagFromSlug")


class ResponsePost(_Model):
    id: str | None = None
    creator: UserNode | None = None


class ThreadedPostResponsesConnection(_Model):
    posts: list[ResponsePost] = Field(default_factory=list)


class PostResponsesPost(_Model):
    threaded_post_responses: ThreadedPostResponsesConnection | None = Field(
        default=None,
        alias="threadedPostResponses",
    )


class PostResponsesData(_Model):
    post: PostResponsesPost | None = None


def parse_topic_latest_story_creators(result: GraphQLResult) -> list[tuple[UserNode, str | None, str | None]]:
    payload = TopicLatestStoriesData.model_validate(result.data or {})
    creators: list[tuple[UserNode, str | None, str | None]] = []
    edges = payload.tag_from_slug.posts.edges if payload.tag_from_slug and payload.tag_from_slug.posts else []
    for edge in edges:
        if not edge.node or not edge.node.creator:
            continue
        creators.append((edge.node.creator, edge.node.id, edge.node.title))
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


def parse_latest_post_preview(result: GraphQLResult) -> tuple[str | None, str | None]:
    payload = UserLatestPostData.model_validate(result.data or {})
    if not payload.user_result or not payload.user_result.homepage_posts_connection:
        return None, None
    posts = payload.user_result.homepage_posts_connection.posts
    if not posts:
        return None, None
    post = posts[0]
    return post.id, post.title


def parse_recent_post_contexts(
    result: GraphQLResult,
) -> list[tuple[str, str | None, str | None, list[tuple[str, str | int | None, str, list[dict[str, int | str | None]]]]]]:
    payload = UserLatestPostData.model_validate(result.data or {})
    if not payload.user_result or not payload.user_result.homepage_posts_connection:
        return []
    posts = payload.user_result.homepage_posts_connection.posts
    if not posts:
        return []

    contexts: list[tuple[str, str | None, str | None, list[tuple[str, str | int | None, str, list[dict[str, int | str | None]]]]]] = []
    for post in posts:
        if not post.id:
            continue
        version_id = post.latest_published_version_id
        if not version_id and post.latest_published_version:
            latest_version = post.latest_published_version
            if isinstance(latest_version, str):
                version_id = latest_version
            else:
                version_id = latest_version.id

        paragraphs: list[tuple[str, str | int | None, str, list[dict[str, int | str | None]]]] = []
        body_model = post.content.body_model if post.content else None
        if body_model:
            for paragraph in body_model.paragraphs:
                name = (paragraph.name or "").strip()
                text = paragraph.text or ""
                if not name or not text.strip():
                    continue
                markups = [
                    markup.model_dump()
                    for markup in paragraph.markups
                    if markup.type and markup.start is not None and markup.end is not None
                ]
                paragraphs.append((name, paragraph.type, text, markups))

        contexts.append((post.id, post.title, version_id, paragraphs))

    return contexts


def parse_latest_post_context(
    result: GraphQLResult,
) -> tuple[str | None, str | None, str | None, list[tuple[str, str]]]:
    contexts = parse_recent_post_contexts(result)
    if not contexts:
        return None, None, None, []
    post_id, title, version_id, paragraphs_with_type = contexts[0]
    paragraphs = [(name, text) for name, _paragraph_type, text, _markups in paragraphs_with_type]
    return post_id, title, version_id, paragraphs


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


def parse_user_viewer_user_node(result: GraphQLResult) -> UserNode | None:
    payload = UserViewerEdgeData.model_validate(result.data or {})
    if not payload.user or not payload.user.id:
        return None
    return UserNode(
        id=payload.user.id,
        username=payload.user.username,
        name=payload.user.name,
        bio=payload.user.bio,
        socialStats=payload.user.social_stats.model_dump(by_alias=True) if payload.user.social_stats else None,
        newsletterV3=payload.user.newsletter_v3.model_dump(by_alias=True) if payload.user.newsletter_v3 else None,
    )


def parse_user_viewer_last_post_created_at(result: GraphQLResult) -> str | None:
    payload = UserViewerEdgeData.model_validate(result.data or {})
    if not payload.user or not payload.user.viewer_edge:
        return None
    value = payload.user.viewer_edge.last_post_created_at
    if value is None:
        return None
    text = str(value).strip()
    return text or None


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


def parse_create_quote_id(result: GraphQLResult) -> str | None:
    payload = QuoteCreateMutationData.model_validate(result.data or {})
    if not payload.create_quote:
        return None
    return payload.create_quote.id


def parse_delete_quote_success(result: GraphQLResult) -> bool | None:
    payload = DeleteQuoteMutationData.model_validate(result.data or {})
    return payload.delete_quote


def parse_topic_curated_list_users(result: GraphQLResult) -> list[tuple[UserNode, str | None, str | None]]:
    payload = TopicCuratedListData.model_validate(result.data or {})
    if not payload.tag_from_slug or not payload.tag_from_slug.curated_lists:
        return []
    users: list[tuple[UserNode, str | None, str | None]] = []
    for edge in payload.tag_from_slug.curated_lists.edges:
        if not edge.node or not edge.node.items_connection:
            continue
        for item in edge.node.items_connection.items:
            if not item.entity or not item.entity.creator:
                continue
            users.append((item.entity.creator, item.entity.id, item.entity.title))
    return users


def parse_post_response_creators(result: GraphQLResult) -> list[UserNode]:
    payload = PostResponsesData.model_validate(result.data or {})
    if not payload.post or not payload.post.threaded_post_responses:
        return []
    users: list[UserNode] = []
    for post in payload.post.threaded_post_responses.posts:
        if not post.creator:
            continue
        users.append(post.creator)
    return users
