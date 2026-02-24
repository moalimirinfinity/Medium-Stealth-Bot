from datetime import datetime
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
    probe: ProbeSnapshot | None = None
