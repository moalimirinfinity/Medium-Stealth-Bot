from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, computed_field


class GraphQLOperation(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    operation_name: str = Field(alias="operationName")
    query: str
    variables: dict[str, Any] = Field(default_factory=dict)


class GraphQLError(BaseModel):
    message: str
    path: list[str | int] | None = None
    extensions: dict[str, Any] | None = None


class GraphQLResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    operation_name: str = Field(alias="operationName")
    status_code: int = Field(alias="statusCode")
    data: dict[str, Any] | None = None
    errors: list[GraphQLError] = Field(default_factory=list)
    raw: Any = None
    stubbed: bool = False

    @computed_field
    @property
    def has_errors(self) -> bool:
        return bool(self.errors)


class AuthSessionMaterial(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    medium_session: str = Field(alias="MEDIUM_SESSION")
    medium_csrf: str | None = Field(default=None, alias="MEDIUM_CSRF")
    medium_user_ref: str | None = Field(default=None, alias="MEDIUM_USER_REF")
    cookie_names: list[str] = Field(default_factory=list)


class ProbeSnapshot(BaseModel):
    tag_slug: str
    started_at: datetime
    duration_ms: int
    results: dict[str, GraphQLResult]


class DailyRunOutcome(BaseModel):
    budget_exhausted: bool
    actions_today: int
    max_actions_per_day: int
    dry_run: bool = True
    considered_candidates: int = 0
    eligible_candidates: int = 0
    follow_actions_attempted: int = 0
    follow_actions_verified: int = 0
    cleanup_actions_attempted: int = 0
    cleanup_actions_verified: int = 0
    decision_log: list[str] = Field(default_factory=list)
    probe: ProbeSnapshot | None = None


class NewsletterState(StrEnum):
    SUBSCRIBED = "subscribed"
    UNSUBSCRIBED = "unsubscribed"
    UNKNOWN = "unknown"


class UserFollowState(StrEnum):
    FOLLOWING = "following"
    NOT_FOLLOWING = "not_following"
    UNKNOWN = "unknown"


class RelationshipConfidence(StrEnum):
    OBSERVED = "observed"
    INFERRED = "inferred"
    STUBBED = "stubbed"


class CanonicalRelationshipState(BaseModel):
    user_id: str
    newsletter_state: NewsletterState = NewsletterState.UNKNOWN
    user_follow_state: UserFollowState = UserFollowState.UNKNOWN
    confidence: RelationshipConfidence = RelationshipConfidence.OBSERVED
    last_source_operation: str | None = None
    updated_at: datetime | None = None
    last_verified_at: datetime | None = None


class CandidateSource(StrEnum):
    TOPIC_LATEST_STORIES = "topic_latest_stories"
    TOPIC_WHO_TO_FOLLOW = "topic_who_to_follow"
    SEED_FOLLOWERS = "seed_followers"


class CandidateUser(BaseModel):
    user_id: str
    username: str | None = None
    name: str | None = None
    bio: str | None = None
    newsletter_v3_id: str | None = None
    follower_count: int | None = None
    following_count: int | None = None
    latest_post_id: str | None = None
    score: float = 0.0
    matched_keywords: list[str] = Field(default_factory=list)
    sources: list[CandidateSource] = Field(default_factory=list)


class CandidateDecision(BaseModel):
    user_id: str
    username: str | None = None
    eligible: bool
    reason: str
    score: float = 0.0
