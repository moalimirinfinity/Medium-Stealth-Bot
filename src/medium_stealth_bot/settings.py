from pathlib import Path
from typing import Literal

from pydantic import Field, computed_field
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
    min_read_wait_seconds: int = Field(default=30, ge=0, validation_alias="MIN_READ_WAIT_SECONDS")
    max_read_wait_seconds: int = Field(default=90, ge=0, validation_alias="MAX_READ_WAIT_SECONDS")

    playwright_profile_dir: Path = Field(
        default=Path(".data/playwright-profile"),
        validation_alias="PLAYWRIGHT_PROFILE_DIR",
    )
    playwright_headless: bool = Field(default=True, validation_alias="PLAYWRIGHT_HEADLESS")

    @computed_field
    @property
    def has_session(self) -> bool:
        return bool(self.medium_session)

    def ensure_directories(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.playwright_profile_dir.mkdir(parents=True, exist_ok=True)
