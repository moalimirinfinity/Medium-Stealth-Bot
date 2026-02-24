from pathlib import Path
from typing import Literal

from pydantic import Field, ValidationInfo, computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    medium_session: str | None = Field(default=None, validation_alias="MEDIUM_SESSION")
    medium_csrf: str | None = Field(default=None, validation_alias="MEDIUM_CSRF")
    medium_user_ref: str | None = Field(default=None, validation_alias="MEDIUM_USER_REF")

    app_env: str = Field(default="dev", validation_alias="APP_ENV")
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")
    client_mode: Literal["stealth", "fast"] = Field(default="stealth", validation_alias="CLIENT_MODE")
    day_boundary_policy: Literal["utc"] = Field(default="utc", validation_alias="DAY_BOUNDARY_POLICY")

    data_dir: Path = Field(default=Path(".data"), validation_alias="DATA_DIR")
    db_path: Path = Field(default=Path(".data/medium-stealth-bot.db"), validation_alias="DB_PATH")

    graphql_endpoint: str = Field(
        default="https://medium.com/_/graphql",
        validation_alias="GRAPHQL_ENDPOINT",
    )
    graphql_origin: str = Field(default="https://medium.com", validation_alias="GRAPHQL_ORIGIN")
    graphql_referer: str = Field(default="https://medium.com/", validation_alias="GRAPHQL_REFERER")
    apollo_client_name: str = Field(
        default="lite",
        validation_alias="APOLLOGRAPHQL_CLIENT_NAME",
    )
    apollo_client_version: str = Field(
        default="main-20260223-200902-cb62c3b9f7",
        validation_alias="APOLLOGRAPHQL_CLIENT_VERSION",
    )
    user_agent: str = Field(default=DEFAULT_USER_AGENT, validation_alias="USER_AGENT")

    max_actions_per_day: int = Field(
        default=50,
        ge=1,
        le=500,
        validation_alias="MAX_ACTIONS_PER_DAY",
    )
    max_follow_actions_per_run: int = Field(default=5, ge=0, le=200, validation_alias="MAX_FOLLOW_ACTIONS_PER_RUN")
    follow_candidate_limit: int = Field(default=30, ge=1, le=500, validation_alias="FOLLOW_CANDIDATE_LIMIT")
    follow_cooldown_hours: int = Field(default=72, ge=1, le=24 * 60, validation_alias="FOLLOW_COOLDOWN_HOURS")
    min_following_follower_ratio: float = Field(
        default=0.5,
        ge=0.0,
        le=100.0,
        validation_alias="MIN_FOLLOWING_FOLLOWER_RATIO",
    )
    require_bio_keyword_match: bool = Field(default=False, validation_alias="REQUIRE_BIO_KEYWORD_MATCH")
    bio_keywords_raw: str = Field(
        default="coding,software,engineer,developer,python,javascript,react",
        validation_alias="BIO_KEYWORDS",
    )
    discovery_seed_users_raw: str = Field(default="", validation_alias="DISCOVERY_SEED_USERS")
    discovery_seed_followers_limit: int = Field(default=8, ge=1, le=100, validation_alias="DISCOVERY_SEED_FOLLOWERS_LIMIT")
    discovery_followers_depth: int = Field(default=1, ge=1, le=2, validation_alias="DISCOVERY_FOLLOWERS_DEPTH")
    discovery_second_hop_seed_limit: int = Field(
        default=3,
        ge=1,
        le=50,
        validation_alias="DISCOVERY_SECOND_HOP_SEED_LIMIT",
    )
    unfollow_nonreciprocal_after_days: int = Field(
        default=7,
        ge=1,
        le=180,
        validation_alias="UNFOLLOW_NONRECIPROCAL_AFTER_DAYS",
    )
    cleanup_unfollow_limit: int = Field(default=10, ge=0, le=100, validation_alias="CLEANUP_UNFOLLOW_LIMIT")
    own_followers_scan_limit: int = Field(default=80, ge=1, le=500, validation_alias="OWN_FOLLOWERS_SCAN_LIMIT")

    enable_pre_follow_clap: bool = Field(default=True, validation_alias="ENABLE_PRE_FOLLOW_CLAP")
    pre_follow_read_wait_seconds: int = Field(default=60, ge=0, le=600, validation_alias="PRE_FOLLOW_READ_WAIT_SECONDS")
    min_clap_count: int = Field(default=12, ge=1, le=50, validation_alias="MIN_CLAP_COUNT")
    max_clap_count: int = Field(default=40, ge=1, le=50, validation_alias="MAX_CLAP_COUNT")

    min_read_wait_seconds: int = Field(default=30, ge=0, validation_alias="MIN_READ_WAIT_SECONDS")
    max_read_wait_seconds: int = Field(default=90, ge=0, validation_alias="MAX_READ_WAIT_SECONDS")
    min_action_gap_seconds: int = Field(default=30, ge=0, validation_alias="MIN_ACTION_GAP_SECONDS")
    max_action_gap_seconds: int = Field(default=90, ge=0, validation_alias="MAX_ACTION_GAP_SECONDS")

    playwright_profile_dir: Path = Field(
        default=Path(".data/playwright-profile"),
        validation_alias="PLAYWRIGHT_PROFILE_DIR",
    )
    playwright_headless: bool = Field(default=True, validation_alias="PLAYWRIGHT_HEADLESS")

    @computed_field
    @property
    def has_session(self) -> bool:
        return bool(self.medium_session)

    @field_validator("medium_user_ref")
    @classmethod
    def validate_medium_user_ref(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            return None
        if normalized.startswith("@"):
            raise ValueError("MEDIUM_USER_REF must be a Medium user_id, not an @username.")
        return normalized

    @field_validator("bio_keywords_raw", "discovery_seed_users_raw")
    @classmethod
    def normalize_csv_env(cls, value: str) -> str:
        return value.strip()

    @field_validator("max_clap_count")
    @classmethod
    def validate_clap_range(cls, value: int, info: ValidationInfo) -> int:
        min_value = info.data.get("min_clap_count")
        if min_value is not None and value < min_value:
            raise ValueError("MAX_CLAP_COUNT must be greater than or equal to MIN_CLAP_COUNT.")
        return value

    @field_validator("max_read_wait_seconds", "max_action_gap_seconds")
    @classmethod
    def validate_max_at_least_min(cls, value: int, info: ValidationInfo) -> int:
        min_field = "min_read_wait_seconds" if info.field_name == "max_read_wait_seconds" else "min_action_gap_seconds"
        min_value = info.data.get(min_field)
        if min_value is not None and value < min_value:
            raise ValueError(f"{info.field_name} must be greater than or equal to {min_field}.")
        return value

    @computed_field
    @property
    def bio_keywords(self) -> list[str]:
        return [item.strip().lower() for item in self.bio_keywords_raw.split(",") if item.strip()]

    @computed_field
    @property
    def discovery_seed_users(self) -> list[str]:
        return [item.strip() for item in self.discovery_seed_users_raw.split(",") if item.strip()]

    def ensure_directories(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.playwright_profile_dir.mkdir(parents=True, exist_ok=True)
