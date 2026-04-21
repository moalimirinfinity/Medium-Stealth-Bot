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


class GrowthMode(StrEnum):
    SIMPLE = "simple"
    SMART = "smart"


class GrowthDiscoveryMode(StrEnum):
    GENERAL = "general"
    TARGET_USER_FOLLOWERS = "target-user-followers"


class GrowthPolicy(StrEnum):
    FOLLOW_ONLY = "follow-only"
    WARM_ENGAGE = "warm-engage"
    WARM_ENGAGE_RARE_COMMENT = "warm-engage-plus-rare-comment"


class GrowthSource(StrEnum):
    TOPIC_RECOMMENDED = "topic-recommended"
    SEED_FOLLOWERS = "seed-followers"
    TARGET_USER_FOLLOWERS = "target-user-followers"
    PUBLICATION_ADJACENT = "publication-adjacent"
    RESPONDERS = "responders"


class DailyRunOutcome(BaseModel):
    budget_exhausted: bool
    actions_today: int
    max_actions_per_day: int
    cleanup_only_mode: bool = False
    growth_policy: GrowthPolicy | None = None
    growth_sources: list[GrowthSource] = Field(default_factory=list)
    growth_mode: GrowthMode | None = None
    discovery_mode: GrowthDiscoveryMode | None = None
    target_user_refs: list[str] = Field(default_factory=list)
    target_user_scan_limit: int | None = None
    action_counts_today: dict[str, int] = Field(default_factory=dict)
    action_limits_per_day: dict[str, int] = Field(default_factory=dict)
    action_remaining_per_day: dict[str, int] = Field(default_factory=dict)
    dry_run: bool = True
    considered_candidates: int = 0
    eligible_candidates: int = 0
    follow_actions_attempted: int = 0
    follow_actions_verified: int = 0
    clap_actions_attempted: int = 0
    clap_actions_verified: int = 0
    comment_actions_attempted: int = 0
    comment_actions_verified: int = 0
    cleanup_actions_attempted: int = 0
    cleanup_actions_verified: int = 0
    source_candidate_counts: dict[str, int] = Field(default_factory=dict)
    source_follow_verified_counts: dict[str, int] = Field(default_factory=dict)
    policy_follow_verified_counts: dict[str, int] = Field(default_factory=dict)
    conversion_by_source: dict[str, dict[str, float | int]] = Field(default_factory=dict)
    conversion_by_policy: dict[str, dict[str, float | int]] = Field(default_factory=dict)
    conversion_by_source_policy: dict[str, dict[str, float | int]] = Field(default_factory=dict)
    kpis: dict[str, float | int] = Field(default_factory=dict)
    client_metrics: dict[str, Any] = Field(default_factory=dict)
    decision_log: list[str] = Field(default_factory=list)
    decision_reason_counts: dict[str, int] = Field(default_factory=dict)
    decision_result_counts: dict[str, int] = Field(default_factory=dict)
    probe: ProbeSnapshot | None = None
    session_passes: int = 1
    session_elapsed_seconds: float = 0.0
    session_stop_reason: str | None = None
    session_target_follow_attempts: int | None = None
    session_target_duration_minutes: int | None = None


class ReconcileOutcome(BaseModel):
    dry_run: bool = True
    scanned_users: int = 0
    updated_users: int = 0
    following_count: int = 0
    not_following_count: int = 0
    unknown_count: int = 0
    decision_log: list[str] = Field(default_factory=list)


class GraphSyncOutcome(BaseModel):
    dry_run: bool = True
    mode: str = "auto"
    run_id: int | None = None
    skipped: bool = False
    skip_reason: str | None = None
    followers_count: int = 0
    following_count: int = 0
    users_upserted_count: int = 0
    imported_pending_count: int = 0
    source_path: str | None = None
    used_following_source: str | None = None
    duration_ms: int = 0


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
    WHO_TO_FOLLOW_MODULE = "who_to_follow_module"
    TOPIC_CURATED_LIST = "topic_curated_list"
    SEED_FOLLOWERS = "seed_followers"
    TARGET_USER_FOLLOWERS = "target_user_followers"
    POST_RESPONDERS = "post_responders"


class CandidateUser(BaseModel):
    user_id: str
    username: str | None = None
    name: str | None = None
    bio: str | None = None
    newsletter_v3_id: str | None = None
    follower_count: int | None = None
    following_count: int | None = None
    latest_post_id: str | None = None
    latest_post_title: str | None = None
    last_post_created_at: str | None = None
    score: float = 0.0
    matched_keywords: list[str] = Field(default_factory=list)
    sources: list[CandidateSource] = Field(default_factory=list)


class CandidateDecision(BaseModel):
    user_id: str
    username: str | None = None
    eligible: bool
    reason: str
    score: float = 0.0
