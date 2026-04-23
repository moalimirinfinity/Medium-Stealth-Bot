import time
from typing import Any, Sequence

import structlog
from medium_stealth_bot.browser_runtime import parse_cookie_header
from medium_stealth_bot.identity import resolve_browser_identity, resolve_csrf_token

from medium_stealth_bot.contract_registry import (
    OperationContractRegistry,
    load_operation_contract_registry,
)
from medium_stealth_bot.models import GraphQLError, GraphQLOperation, GraphQLResult
from medium_stealth_bot.settings import AppSettings
from medium_stealth_bot.transport import (
    CurlCffiGraphQLTransport,
    GraphQLTransport,
    PlaywrightGraphQLTransport,
)


class MediumAsyncClient:
    def __init__(self, settings: AppSettings):
        self.settings = settings
        self.log = structlog.get_logger(__name__)
        self._contract_registry = self._load_contract_registry()
        self._headers: dict[str, str] = {}
        self._transport: GraphQLTransport | None = None
        self._request_count = 0
        self._request_latency_total_ms = 0.0
        self._result_failures = 0
        self._status_counts: dict[int, int] = {}
        self._last_status_code: int | None = None
        self._last_operation_names: list[str] = []
        self._last_response_headers: dict[str, str] = {}

    async def __aenter__(self) -> "MediumAsyncClient":
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def open(self) -> None:
        if self._transport is not None:
            return

        cookie_map = parse_cookie_header(self.settings.medium_session or "")
        if self.settings.medium_session and "sid" not in cookie_map:
            raise RuntimeError(
                "MEDIUM_SESSION is set but missing `sid` cookie. "
                "Refresh auth with `uv run bot auth`."
            )
        identity = resolve_browser_identity(self.settings)
        csrf = resolve_csrf_token(self.settings, cookie_map)
        headers = identity.graphql_headers(csrf_token=csrf)
        self._headers = headers

        transport: GraphQLTransport
        if self.settings.client_mode == "fast":
            if "sid" not in cookie_map:
                raise RuntimeError(
                    "CLIENT_MODE=fast requires MEDIUM_SESSION with a valid `sid` cookie. "
                    "Run `uv run bot auth` to refresh session cookies."
                )
            transport = CurlCffiGraphQLTransport(
                endpoint=self.settings.graphql_endpoint,
                impersonate=identity.curl_impersonate,
                headers=headers,
                cookie_map=cookie_map,
            )
        else:
            transport = PlaywrightGraphQLTransport(
                profile_dir=self.settings.playwright_profile_dir,
                headless=self.settings.playwright_headless,
                user_agent=identity.user_agent,
                channel=identity.playwright_channel,
                endpoint=self.settings.graphql_endpoint,
                headers=headers,
                cookie_map=cookie_map,
            )

        try:
            await transport.open()
        except Exception:
            self._transport = None
            raise
        self._transport = transport

        self.log.info(
            "client_opened",
            mode=self.settings.client_mode,
            endpoint=self.settings.graphql_endpoint,
            identity_user_agent=identity.user_agent,
            identity_channel=identity.playwright_channel,
            **(
                {"profile_dir": str(self.settings.playwright_profile_dir)}
                if self.settings.client_mode != "fast"
                else {}
            ),
        )

    async def close(self) -> None:
        if self._transport is not None:
            await self._transport.close()
            self._transport = None
        self.log.info("client_closed", mode=self.settings.client_mode)

    async def reset_transport(self) -> None:
        await self.close()
        await self.open()

    async def execute(self, operation: GraphQLOperation) -> GraphQLResult:
        results = await self.execute_batch([operation])
        return results[0]

    async def execute_batch(self, operations: Sequence[GraphQLOperation]) -> list[GraphQLResult]:
        if not operations:
            return []
        if self._transport is None:
            await self.open()
        if self._transport is None:
            raise RuntimeError("HTTP session is not initialized")

        self._validate_outgoing_operations(operations)
        payload = [op.model_dump(by_alias=True) for op in operations]
        started = time.perf_counter()
        status, headers, raw_json = await self._post_graphql(payload)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        stubbed = headers.get("x-codex-stubbed") == "1"

        if isinstance(raw_json, list):
            raw_items = raw_json
        elif isinstance(raw_json, dict):
            raw_items = [raw_json]
        else:
            raw_items = [{} for _ in operations]

        results: list[GraphQLResult] = []
        for idx, operation in enumerate(operations):
            raw_item = raw_items[idx] if idx < len(raw_items) and isinstance(raw_items[idx], dict) else {}
            raw_errors = raw_item.get("errors", []) if isinstance(raw_item, dict) else []
            errors = [GraphQLError.model_validate(item) for item in raw_errors if isinstance(item, dict)]
            data = raw_item.get("data") if isinstance(raw_item, dict) else None
            result = GraphQLResult(
                operationName=operation.operation_name,
                statusCode=status,
                data=data if isinstance(data, dict) else None,
                errors=errors,
                raw=raw_item,
                stubbed=stubbed,
            )
            if self.settings.contract_registry_validate_response_fields:
                self._log_response_contract_mismatch(operation.operation_name, result)
            if result.status_code != 200 or result.has_errors:
                self._result_failures += 1
            results.append(result)

        self._request_count += 1
        self._request_latency_total_ms += elapsed_ms
        self._status_counts[status] = self._status_counts.get(status, 0) + 1
        self._last_status_code = status
        self._last_operation_names = [operation.operation_name for operation in operations]
        self._last_response_headers = self._diagnostic_headers(headers)
        self.log.info(
            "graphql_batch_executed",
            operation_count=len(operations),
            status_code=status,
            stubbed=stubbed,
            mode=self.settings.client_mode,
            latency_ms=round(elapsed_ms, 3),
        )
        return results

    async def _post_graphql(self, payload: list[dict[str, Any]]) -> tuple[int, dict[str, str], Any]:
        if self._transport is None:
            raise RuntimeError("HTTP session is not initialized")
        return await self._transport.post_graphql(payload)

    @staticmethod
    def _diagnostic_headers(headers: dict[str, str]) -> dict[str, str]:
        if not headers:
            return {}
        keep_explicit = {
            "cf-cache-status",
            "cf-ray",
            "content-type",
            "retry-after",
            "server",
            "x-envoy-upstream-service-time",
            "x-request-id",
            "x-request-received-at",
        }
        filtered: dict[str, str] = {}
        for key, value in headers.items():
            lowered = str(key).lower()
            if (
                lowered in keep_explicit
                or "rate" in lowered
                or "limit" in lowered
                or "retry" in lowered
                or lowered.startswith("cf-")
                or lowered.startswith("x-ratelimit")
            ):
                text = str(value)
                filtered[lowered] = text if len(text) <= 300 else f"{text[:300]}..."
        return dict(sorted(filtered.items()))

    def _load_contract_registry(self) -> OperationContractRegistry:
        path = self.settings.implementation_ops_registry_path
        try:
            registry = load_operation_contract_registry(
                path=path,
                strict=self.settings.contract_registry_strict,
            )
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                f"Failed to load operation contract registry from {path}: {exc}"
            ) from exc

        self.log.info(
            "operation_contract_registry_loaded",
            path=str(path),
            strict=self.settings.contract_registry_strict,
            operation_count=registry.operation_count,
        )
        return registry

    def _validate_outgoing_operations(self, operations: Sequence[GraphQLOperation]) -> None:
        violations: list[dict[str, Any]] = []
        for operation in operations:
            issues = self._contract_registry.validate_request(operation)
            if issues:
                violations.append(
                    {
                        "operationName": operation.operation_name,
                        "issues": issues,
                    }
                )
        if not violations:
            return

        self.log.error(
            "operation_contract_request_validation_failed",
            strict=self.settings.contract_registry_strict,
            violations=violations,
        )
        if self.settings.contract_registry_strict:
            details = "; ".join(
                f"{item['operationName']}[{','.join(item['issues'])}]"
                for item in violations
            )
            raise ValueError(f"Operation contract validation failed: {details}")

    def _log_response_contract_mismatch(self, operation_name: str, result: GraphQLResult) -> None:
        missing_paths = self._contract_registry.validate_response(operation_name, result.data)
        if not missing_paths:
            return
        self.log.warning(
            "operation_contract_response_mismatch",
            operation_name=operation_name,
            missing_paths=missing_paths,
            status_code=result.status_code,
            has_errors=result.has_errors,
            stubbed=result.stubbed,
        )

    def metrics_snapshot(self) -> dict[str, Any]:
        average_latency_ms = (
            self._request_latency_total_ms / self._request_count if self._request_count > 0 else 0.0
        )
        return {
            "mode": self.settings.client_mode,
            "request_count": self._request_count,
            "avg_latency_ms": round(average_latency_ms, 3),
            "status_counts": {str(code): count for code, count in sorted(self._status_counts.items())},
            "result_failures": self._result_failures,
            "last_status_code": self._last_status_code,
            "last_operation_names": list(self._last_operation_names),
            "last_response_headers": dict(self._last_response_headers),
        }
