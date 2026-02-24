from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from medium_stealth_bot.models import GraphQLOperation


class OperationContract(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    operation_name: str = Field(alias="operationName")
    classification: list[str] = Field(default_factory=list)
    risk_level: str | None = Field(default=None, alias="riskLevel")
    required_variable_keys: list[str] = Field(default_factory=list, alias="requiredVariableKeys")
    optional_variable_keys: list[str] = Field(default_factory=list, alias="optionalVariableKeys")
    expected_top_level_response_fields: list[str] = Field(default_factory=list, alias="expectedTopLevelResponseFields")


class OperationRegistry(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    captured_at: str | None = Field(default=None, alias="capturedAt")
    source_capture: str | None = Field(default=None, alias="sourceCapture")
    endpoint: str | None = None
    registry_version: int | None = Field(default=None, alias="registryVersion")
    core_operations: list[OperationContract] = Field(default_factory=list, alias="coreOperations")

    def operation_map(self) -> dict[str, OperationContract]:
        return {item.operation_name: item for item in self.core_operations}


def _has_data_path(data: dict[str, Any], path: str) -> bool:
    if not path:
        return False
    nodes: list[Any] = [data]
    for segment in path.split("."):
        next_nodes: list[Any] = []
        for node in nodes:
            if isinstance(node, dict):
                if segment in node:
                    next_nodes.append(node[segment])
                continue
            if isinstance(node, list):
                for item in node:
                    if isinstance(item, dict) and segment in item:
                        next_nodes.append(item[segment])
        if not next_nodes:
            return False
        nodes = next_nodes
    return True


class OperationContractRegistry:
    def __init__(self, registry: OperationRegistry, *, strict: bool = True):
        self.registry = registry
        self.strict = strict
        self._map = registry.operation_map()

    @property
    def operation_count(self) -> int:
        return len(self._map)

    def validate_request(self, operation: GraphQLOperation) -> list[str]:
        contract = self._map.get(operation.operation_name)
        if contract is None:
            if self.strict:
                return ["operation_not_in_registry"]
            return []

        issues: list[str] = []
        variables = operation.variables or {}

        for key in contract.required_variable_keys:
            if key not in variables:
                issues.append(f"missing_required_variable:{key}")
                continue
            value = variables.get(key)
            if value is None:
                issues.append(f"null_required_variable:{key}")

        allowed = set(contract.required_variable_keys) | set(contract.optional_variable_keys)
        if allowed:
            unexpected = sorted(key for key in variables if key not in allowed)
            for key in unexpected:
                issues.append(f"unexpected_variable:{key}")

        return issues

    def validate_response(self, operation_name: str, data: dict[str, Any] | None) -> list[str]:
        if not data:
            return []
        contract = self._map.get(operation_name)
        if contract is None:
            return []
        missing: list[str] = []
        for path in contract.expected_top_level_response_fields:
            if not _has_data_path(data, path):
                missing.append(path)
        return missing


def load_operation_contract_registry(path: Path, *, strict: bool = True) -> OperationContractRegistry:
    payload = json.loads(path.read_text(encoding="utf-8"))
    registry = OperationRegistry.model_validate(payload)
    return OperationContractRegistry(registry=registry, strict=strict)
