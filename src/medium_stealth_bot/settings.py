from pathlib import Path
from typing import Literal

from pydantic import Field, ValidationInfo, computed_field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/142.0.0.0 Safari/537.36"
)


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    medium_session: str | None = Field(default=None, validation_alias="MEDIUM_SESSION")
    medium_session_sid: str | None = Field(default=None, validation_alias="MEDIUM_SESSION_SID")
    medium_session_uid: str | None = Field(default=None, validation_alias="MEDIUM_SESSION_UID")
    medium_session_xsrf: str | None = Field(default=None, validation_alias="MEDIUM_SESSION_XSRF")
    medium_session_cf_clearance: str | None = Field(default=None, validation_alias="MEDIUM_SESSION_CF_CLEARANCE")
    medium_session_cfuvid: str | None = Field(default=None, validation_alias="MEDIUM_SESSION_CFUVID")
    medium_csrf: str | None = Field(default=None, validation_alias="MEDIUM_CSRF")
    medium_user_ref: str | None = Field(default=None, validation_alias="MEDIUM_USER_REF")

    app_env: str = Field(default="dev", validation_alias="APP_ENV")
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")
    log_format: Literal["pretty", "json"] = Field(default="pretty", validation_alias="LOG_FORMAT")
    client_mode: Literal["stealth", "fast"] = Field(default="stealth", validation_alias="CLIENT_MODE")
    day_boundary_policy: Literal["utc"] = Field(default="utc", validation_alias="DAY_BOUNDARY_POLICY")

    data_dir: Path = Field(default=Path(".data"), validation_alias="DATA_DIR")
    db_path: Path = Field(default=Path(".data/medium-stealth-bot.db"), validation_alias="DB_PATH")
    run_artifacts_dir: Path = Field(default=Path(".data/runs"), validation_alias="RUN_ARTIFACTS_DIR")

    graphql_endpoint: str = Field(
        default="https://medium.com/_/graphql",
        validation_alias="GRAPHQL_ENDPOINT",
    )
    graphql_origin: str = Field(default="https://medium.com", validation_alias="GRAPHQL_ORIGIN")
    graphql_referer: str = Field(default="https://medium.com/", validation_alias="GRAPHQL_REFERER")
    implementation_ops_registry_path: Path = Field(
        default=Path("captures/final/implementation_ops_2026-02-24.json"),
        validation_alias="IMPLEMENTATION_OPS_REGISTRY_PATH",
    )
    contract_registry_strict: bool = Field(default=True, validation_alias="CONTRACT_REGISTRY_STRICT")
    contract_registry_validate_response_fields: bool = Field(
        default=False,
        validation_alias="CONTRACT_REGISTRY_VALIDATE_RESPONSE_FIELDS",
    )
    contract_registry_live_newsletter_slug: str | None = Field(
        default=None,
        validation_alias="CONTRACT_REGISTRY_LIVE_NEWSLETTER_SLUG",
    )
    contract_registry_live_newsletter_username: str | None = Field(
        default=None,
        validation_alias="CONTRACT_REGISTRY_LIVE_NEWSLETTER_USERNAME",
    )
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
        validation_alias="MAX_ACTIONS_PER_DAY",
    )
    max_subscribe_actions_per_day: int = Field(
        default=30,
        ge=0,
        validation_alias="MAX_SUBSCRIBE_ACTIONS_PER_DAY",
    )
    max_unfollow_actions_per_day: int = Field(
        default=20,
        ge=0,
        validation_alias="MAX_UNFOLLOW_ACTIONS_PER_DAY",
    )
    max_clap_actions_per_day: int = Field(
        default=40,
        ge=0,
        validation_alias="MAX_CLAP_ACTIONS_PER_DAY",
    )
    live_session_duration_minutes: int = Field(
        default=60,
        ge=1,
        le=24 * 12,
        validation_alias="LIVE_SESSION_DURATION_MINUTES",
    )
    live_session_target_follow_attempts: int = Field(
        default=100,
        ge=1,
        le=5000,
        validation_alias="LIVE_SESSION_TARGET_FOLLOW_ATTEMPTS",
    )
    live_session_min_follow_attempts: int = Field(
        default=80,
        ge=1,
        le=5000,
        validation_alias="LIVE_SESSION_MIN_FOLLOW_ATTEMPTS",
    )
    live_session_max_passes: int = Field(
        default=12,
        ge=1,
        le=500,
        validation_alias="LIVE_SESSION_MAX_PASSES",
    )
    max_follow_actions_per_run: int = Field(default=5, ge=0, validation_alias="MAX_FOLLOW_ACTIONS_PER_RUN")
    reconcile_scan_limit: int = Field(default=200, ge=1, le=5000, validation_alias="RECONCILE_SCAN_LIMIT")
    reconcile_page_size: int = Field(default=50, ge=1, le=500, validation_alias="RECONCILE_PAGE_SIZE")
    graph_sync_auto_enabled: bool = Field(default=True, validation_alias="GRAPH_SYNC_AUTO_ENABLED")
    graph_sync_freshness_window_minutes: int = Field(
        default=5,
        ge=0,
        le=24 * 12,
        validation_alias="GRAPH_SYNC_FRESHNESS_WINDOW_MINUTES",
    )
    graph_sync_full_pagination: bool = Field(default=True, validation_alias="GRAPH_SYNC_FULL_PAGINATION")
    graph_sync_enable_graphql_following: bool = Field(
        default=True,
        validation_alias="GRAPH_SYNC_ENABLE_GRAPHQL_FOLLOWING",
    )
    graph_sync_enable_scrape_fallback: bool = Field(
        default=True,
        validation_alias="GRAPH_SYNC_ENABLE_SCRAPE_FALLBACK",
    )
    graph_sync_scrape_page_timeout_seconds: int = Field(
        default=30,
        ge=5,
        le=300,
        validation_alias="GRAPH_SYNC_SCRAPE_PAGE_TIMEOUT_SECONDS",
    )
    follow_candidate_limit: int = Field(default=30, ge=1, le=500, validation_alias="FOLLOW_CANDIDATE_LIMIT")
    follow_cooldown_hours: int = Field(default=72, ge=1, le=24 * 60, validation_alias="FOLLOW_COOLDOWN_HOURS")
    min_following_follower_ratio: float = Field(
        default=0.5,
        ge=0.0,
        le=100.0,
        validation_alias="MIN_FOLLOWING_FOLLOWER_RATIO",
    )
    require_bio_keyword_match: bool = Field(default=False, validation_alias="REQUIRE_BIO_KEYWORD_MATCH")
    score_weight_ratio: float = Field(default=1.0, ge=0.0, le=10.0, validation_alias="SCORE_WEIGHT_RATIO")
    score_weight_keyword: float = Field(default=0.35, ge=0.0, le=10.0, validation_alias="SCORE_WEIGHT_KEYWORD")
    score_weight_source: float = Field(default=0.2, ge=0.0, le=10.0, validation_alias="SCORE_WEIGHT_SOURCE")
    score_weight_newsletter: float = Field(default=0.2, ge=0.0, le=10.0, validation_alias="SCORE_WEIGHT_NEWSLETTER")
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
    cleanup_unfollow_limit: int = Field(default=10, ge=0, le=5000, validation_alias="CLEANUP_UNFOLLOW_LIMIT")
    cleanup_unfollow_whitelist_min_followers: int = Field(
        default=2000,
        ge=0,
        le=2_000_000_000,
        validation_alias="CLEANUP_UNFOLLOW_WHITELIST_MIN_FOLLOWERS",
    )
    cleanup_unfollow_min_gap_seconds: int = Field(
        default=1,
        ge=0,
        validation_alias="CLEANUP_UNFOLLOW_MIN_GAP_SECONDS",
    )
    cleanup_unfollow_max_gap_seconds: int = Field(
        default=5,
        ge=0,
        validation_alias="CLEANUP_UNFOLLOW_MAX_GAP_SECONDS",
    )
    own_followers_scan_limit: int = Field(default=80, ge=1, le=500, validation_alias="OWN_FOLLOWERS_SCAN_LIMIT")

    enable_pre_follow_clap: bool = Field(default=True, validation_alias="ENABLE_PRE_FOLLOW_CLAP")
    pre_follow_read_wait_seconds: int = Field(default=60, ge=0, le=600, validation_alias="PRE_FOLLOW_READ_WAIT_SECONDS")
    min_clap_count: int = Field(default=12, ge=1, le=50, validation_alias="MIN_CLAP_COUNT")
    max_clap_count: int = Field(default=40, ge=1, le=50, validation_alias="MAX_CLAP_COUNT")

    min_read_wait_seconds: int = Field(default=30, ge=0, validation_alias="MIN_READ_WAIT_SECONDS")
    max_read_wait_seconds: int = Field(default=90, ge=0, validation_alias="MAX_READ_WAIT_SECONDS")
    min_verify_gap_seconds: int = Field(default=3, ge=0, validation_alias="MIN_VERIFY_GAP_SECONDS")
    max_verify_gap_seconds: int = Field(default=12, ge=0, validation_alias="MAX_VERIFY_GAP_SECONDS")
    min_action_gap_seconds: int = Field(default=30, ge=0, validation_alias="MIN_ACTION_GAP_SECONDS")
    max_action_gap_seconds: int = Field(default=90, ge=0, validation_alias="MAX_ACTION_GAP_SECONDS")
    max_mutations_per_10_minutes: int = Field(default=24, ge=1, le=500, validation_alias="MAX_MUTATIONS_PER_10_MINUTES")
    min_session_warmup_seconds: int = Field(default=5, ge=0, validation_alias="MIN_SESSION_WARMUP_SECONDS")
    max_session_warmup_seconds: int = Field(default=20, ge=0, validation_alias="MAX_SESSION_WARMUP_SECONDS")
    pass_cooldown_min_seconds: int = Field(default=20, ge=0, validation_alias="PASS_COOLDOWN_MIN_SECONDS")
    pass_cooldown_max_seconds: int = Field(default=90, ge=0, validation_alias="PASS_COOLDOWN_MAX_SECONDS")
    pacing_soft_degrade_cooldown_seconds: int = Field(
        default=180,
        ge=0,
        le=7200,
        validation_alias="PACING_SOFT_DEGRADE_COOLDOWN_SECONDS",
    )
    enable_pacing_auto_clamp: bool = Field(default=True, validation_alias="ENABLE_PACING_AUTO_CLAMP")

    risk_halt_consecutive_failures: int = Field(
        default=3,
        ge=1,
        le=20,
        validation_alias="RISK_HALT_CONSECUTIVE_FAILURES",
    )
    risk_halt_mode: Literal["hard", "soft"] = Field(default="hard", validation_alias="RISK_HALT_MODE")
    enable_challenge_halt: bool = Field(default=True, validation_alias="ENABLE_CHALLENGE_HALT")
    challenge_status_codes_raw: str = Field(default="403,429,503", validation_alias="CHALLENGE_STATUS_CODES")
    challenge_tokens_raw: str = Field(
        default="just a moment,attention required,cloudflare,cf-chl,captcha,turnstile,managed challenge",
        validation_alias="CHALLENGE_DETECTION_TOKENS",
    )
    enable_session_expiry_halt: bool = Field(default=True, validation_alias="ENABLE_SESSION_EXPIRY_HALT")
    session_expiry_status_codes_raw: str = Field(
        default="401,419,440",
        validation_alias="SESSION_EXPIRY_STATUS_CODES",
    )
    session_expiry_tokens_raw: str = Field(
        default="session expired,csrf,unauthorized,authentication required,login required,sign in",
        validation_alias="SESSION_EXPIRY_DETECTION_TOKENS",
    )

    query_max_retries: int = Field(default=1, ge=0, le=8, validation_alias="QUERY_MAX_RETRIES")
    verify_max_retries: int = Field(default=2, ge=0, le=8, validation_alias="VERIFY_MAX_RETRIES")
    mutation_max_retries: int = Field(default=2, ge=0, le=8, validation_alias="MUTATION_MAX_RETRIES")
    retry_base_delay_seconds: float = Field(default=1.0, ge=0.0, le=60.0, validation_alias="RETRY_BASE_DELAY_SECONDS")
    retry_max_delay_seconds: float = Field(default=8.0, ge=0.0, le=300.0, validation_alias="RETRY_MAX_DELAY_SECONDS")
    adaptive_retry_failure_multiplier: float = Field(
        default=0.5,
        ge=0.0,
        le=5.0,
        validation_alias="ADAPTIVE_RETRY_FAILURE_MULTIPLIER",
    )
    operator_kill_switch: bool = Field(default=False, validation_alias="OPERATOR_KILL_SWITCH")

    playwright_profile_dir: Path = Field(
        default=Path(".data/playwright-profile"),
        validation_alias="PLAYWRIGHT_PROFILE_DIR",
    )
    playwright_headless: bool = Field(default=True, validation_alias="PLAYWRIGHT_HEADLESS")
    playwright_auth_browser_channel: Literal["chrome", "chromium"] = Field(
        default="chrome",
        validation_alias="PLAYWRIGHT_AUTH_BROWSER_CHANNEL",
    )

    @computed_field
    @property
    def has_session(self) -> bool:
        return bool(self.medium_session)

    @model_validator(mode="after")
    def compose_medium_session_from_parts(self) -> "AppSettings":
        def _normalized(value: str | None) -> str | None:
            if value is None:
                return None
            text = value.strip()
            return text or None

        self.medium_session = _normalized(self.medium_session)
        self.medium_session_sid = _normalized(self.medium_session_sid)
        self.medium_session_uid = _normalized(self.medium_session_uid)
        self.medium_session_xsrf = _normalized(self.medium_session_xsrf)
        self.medium_session_cf_clearance = _normalized(self.medium_session_cf_clearance)
        self.medium_session_cfuvid = _normalized(self.medium_session_cfuvid)
        self.medium_csrf = _normalized(self.medium_csrf)

        if self.medium_session is None:
            cookie_parts: list[str] = []
            for key, value in (
                ("sid", self.medium_session_sid),
                ("uid", self.medium_session_uid),
                ("xsrf", self.medium_session_xsrf),
                ("cf_clearance", self.medium_session_cf_clearance),
                ("_cfuvid", self.medium_session_cfuvid),
            ):
                if value:
                    cookie_parts.append(f"{key}={value}")
            if cookie_parts:
                self.medium_session = "; ".join(cookie_parts)

        if self.medium_csrf is None and self.medium_session_xsrf is not None:
            self.medium_csrf = self.medium_session_xsrf
        if self.medium_user_ref is None and self.medium_session_uid is not None:
            self.medium_user_ref = self.validate_medium_user_ref(self.medium_session_uid)
        return self

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

    @field_validator(
        "bio_keywords_raw",
        "discovery_seed_users_raw",
        "challenge_status_codes_raw",
        "challenge_tokens_raw",
        "session_expiry_status_codes_raw",
        "session_expiry_tokens_raw",
    )
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

    @field_validator(
        "max_read_wait_seconds",
        "max_verify_gap_seconds",
        "max_action_gap_seconds",
        "cleanup_unfollow_max_gap_seconds",
        "max_session_warmup_seconds",
        "pass_cooldown_max_seconds",
    )
    @classmethod
    def validate_max_at_least_min(cls, value: int, info: ValidationInfo) -> int:
        min_field_map = {
            "max_read_wait_seconds": "min_read_wait_seconds",
            "max_verify_gap_seconds": "min_verify_gap_seconds",
            "max_action_gap_seconds": "min_action_gap_seconds",
            "cleanup_unfollow_max_gap_seconds": "cleanup_unfollow_min_gap_seconds",
            "max_session_warmup_seconds": "min_session_warmup_seconds",
            "pass_cooldown_max_seconds": "pass_cooldown_min_seconds",
        }
        min_field = min_field_map[info.field_name]
        min_value = info.data.get(min_field)
        if min_value is not None and value < min_value:
            raise ValueError(f"{info.field_name} must be greater than or equal to {min_field}.")
        return value

    @field_validator("retry_max_delay_seconds")
    @classmethod
    def validate_retry_delay_range(cls, value: float, info: ValidationInfo) -> float:
        base = info.data.get("retry_base_delay_seconds")
        if base is not None and value < base:
            raise ValueError("RETRY_MAX_DELAY_SECONDS must be greater than or equal to RETRY_BASE_DELAY_SECONDS.")
        return value

    @computed_field
    @property
    def bio_keywords(self) -> list[str]:
        return [item.strip().lower() for item in self.bio_keywords_raw.split(",") if item.strip()]

    @computed_field
    @property
    def discovery_seed_users(self) -> list[str]:
        return [item.strip() for item in self.discovery_seed_users_raw.split(",") if item.strip()]

    @computed_field
    @property
    def challenge_status_codes(self) -> set[int]:
        return self._parse_status_codes(self.challenge_status_codes_raw)

    @computed_field
    @property
    def session_expiry_status_codes(self) -> set[int]:
        return self._parse_status_codes(self.session_expiry_status_codes_raw)

    @computed_field
    @property
    def challenge_tokens(self) -> list[str]:
        return [item.strip().lower() for item in self.challenge_tokens_raw.split(",") if item.strip()]

    @computed_field
    @property
    def session_expiry_tokens(self) -> list[str]:
        return [item.strip().lower() for item in self.session_expiry_tokens_raw.split(",") if item.strip()]

    @staticmethod
    def _parse_status_codes(raw: str) -> set[int]:
        parsed: set[int] = set()
        for token in raw.split(","):
            stripped = token.strip()
            if not stripped or not stripped.isdigit():
                continue
            value = int(stripped)
            if 100 <= value <= 599:
                parsed.add(value)
        return parsed

    def ensure_directories(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.playwright_profile_dir.mkdir(parents=True, exist_ok=True)
        self.run_artifacts_dir.mkdir(parents=True, exist_ok=True)
