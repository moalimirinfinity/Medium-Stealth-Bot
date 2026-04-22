from __future__ import annotations

from typing import Any
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

SUPPORTED_ARTIFACT_SCHEMA_VERSIONS = {1}


class RunArtifactV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    run_id: str
    command: str
    tag_slug: str
    dry_run: bool | None = None
    status: str
    health: str
    started_at: str
    ended_at: str
    duration_ms: int
    summary: dict[str, Any] = Field(default_factory=dict)
    action_counts: dict[str, int] = Field(default_factory=dict)
    result_counts: dict[str, int] = Field(default_factory=dict)
    reason_counts: dict[str, int] = Field(default_factory=dict)
    kpis: dict[str, float | int] = Field(default_factory=dict)
    client_metrics: dict[str, Any] = Field(default_factory=dict)
    source_candidate_counts: dict[str, int] = Field(default_factory=dict)
    source_follow_verified_counts: dict[str, int] = Field(default_factory=dict)
    policy_follow_verified_counts: dict[str, int] = Field(default_factory=dict)
    conversion_by_source: dict[str, dict[str, float | int]] = Field(default_factory=dict)
    conversion_by_policy: dict[str, dict[str, float | int]] = Field(default_factory=dict)
    conversion_by_source_policy: dict[str, dict[str, float | int]] = Field(default_factory=dict)
    decision_log_sample: list[str] = Field(default_factory=list)
    probe: dict[str, Any] | None = None
    error: dict[str, Any] | None = None


def validate_artifact_payload(payload: dict[str, Any]) -> tuple[bool, list[str]]:
    version = payload.get("schema_version")
    if not isinstance(version, int):
        return False, ["missing_or_invalid:schema_version"]
    if version not in SUPPORTED_ARTIFACT_SCHEMA_VERSIONS:
        return False, [f"unsupported_schema_version:{version}"]
    try:
        RunArtifactV1.model_validate(payload)
    except Exception as exc:  # noqa: BLE001
        return False, [f"schema_validation_error:{exc}"]
    return True, []
