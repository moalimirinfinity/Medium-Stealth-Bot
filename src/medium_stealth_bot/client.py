import inspect
import json
from typing import Any, Sequence

import structlog
from curl_cffi import requests as curl_requests
from playwright.async_api import APIRequestContext, BrowserContext, Playwright, async_playwright

from medium_stealth_bot.contract_registry import (
    OperationContractRegistry,
    load_operation_contract_registry,
)
from medium_stealth_bot.models import GraphQLError, GraphQLOperation, GraphQLResult
from medium_stealth_bot.settings import AppSettings


def parse_cookie_header(cookie_header: str) -> dict[str, str]:
    cookies: dict[str, str] = {}
    for pair in cookie_header.split(";"):
        item = pair.strip()
        if not item or "=" not in item:
            continue
        key, value = item.split("=", 1)
        cookies[key.strip()] = value.strip()
    return cookies


class MediumAsyncClient:
    def __init__(self, settings: AppSettings):
        self.settings = settings
        self.log = structlog.get_logger(__name__)
        self._contract_registry = self._load_contract_registry()
        self._headers: dict[str, str] = {}
        self._session: curl_requests.AsyncSession | None = None
        self._playwright: Playwright | None = None
        self._browser_context: BrowserContext | None = None
        self._api_request: APIRequestContext | None = None

    async def __aenter__(self) -> "MediumAsyncClient":
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def open(self) -> None:
        if self._session is not None or self._api_request is not None:
            return

        cookie_map = parse_cookie_header(self.settings.medium_session or "")
        headers = {
            "Origin": self.settings.graphql_origin,
            "Referer": self.settings.graphql_referer,
            "User-Agent": self.settings.user_agent,
            "Content-Type": "application/json",
            "apollographql-client-name": self.settings.apollo_client_name,
            "apollographql-client-version": self.settings.apollo_client_version,
        }
        csrf = self.settings.medium_csrf or cookie_map.get("xsrf") or cookie_map.get("XSRF-TOKEN")
        if csrf:
            headers["x-xsrf-token"] = csrf
        self._headers = headers

        if self.settings.client_mode == "fast":
            self._session = curl_requests.AsyncSession(
                impersonate="chrome142",
                headers=headers,
                cookies=cookie_map,
            )
            self.log.info(
                "client_opened",
                mode=self.settings.client_mode,
                endpoint=self.settings.graphql_endpoint,
            )
            return

        self._playwright = await async_playwright().start()
        self._browser_context = await self._playwright.chromium.launch_persistent_context(
            user_data_dir=str(self.settings.playwright_profile_dir),
            headless=self.settings.playwright_headless,
            viewport={"width": 1280, "height": 800},
            user_agent=self.settings.user_agent,
        )
        if cookie_map:
            cookies = [
                {
                    "name": name,
                    "value": value,
                    "domain": ".medium.com",
                    "path": "/",
                    "secure": True,
                }
                for name, value in cookie_map.items()
            ]
            await self._browser_context.add_cookies(cookies)
        self._api_request = self._browser_context.request
        self.log.info(
            "client_opened",
            mode=self.settings.client_mode,
            endpoint=self.settings.graphql_endpoint,
            profile_dir=str(self.settings.playwright_profile_dir),
        )

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None
        if self._browser_context is not None:
            await self._browser_context.close()
            self._browser_context = None
        if self._playwright is not None:
            await self._playwright.stop()
            self._playwright = None
        self._api_request = None
        self.log.info("client_closed", mode=self.settings.client_mode)

    async def execute(self, operation: GraphQLOperation) -> GraphQLResult:
        results = await self.execute_batch([operation])
        return results[0]

    async def execute_batch(self, operations: Sequence[GraphQLOperation]) -> list[GraphQLResult]:
        if not operations:
            return []
        if self._session is None and self._api_request is None:
            await self.open()
        if self._session is None and self._api_request is None:
            raise RuntimeError("HTTP session is not initialized")

        self._validate_outgoing_operations(operations)
        payload = [op.model_dump(by_alias=True) for op in operations]
        status, headers, raw_json = await self._post_graphql(payload)
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
            results.append(result)

        self.log.info(
            "graphql_batch_executed",
            operation_count=len(operations),
            status_code=status,
            stubbed=stubbed,
            mode=self.settings.client_mode,
        )
        return results

    async def _post_graphql(self, payload: list[dict[str, Any]]) -> tuple[int, dict[str, str], Any]:
        if self.settings.client_mode == "fast":
            if self._session is None:
                raise RuntimeError("Fast client session is not initialized")
            response = await self._session.post(
                self.settings.graphql_endpoint,
                json=payload,
                timeout=45,
            )
            headers = {str(k).lower(): str(v) for k, v in response.headers.items()}
            try:
                raw_json = response.json()
            except Exception:
                raw_text = ""
                try:
                    raw_text = str(response.text or "")
                except Exception:
                    raw_text = ""
                raw_json = {"_raw_text": raw_text[:4000]} if raw_text else {}
            return response.status_code, headers, raw_json

        if self._api_request is None:
            raise RuntimeError("Stealth API context is not initialized")
        response = await self._api_request.post(
            self.settings.graphql_endpoint,
            data=json.dumps(payload),
            headers=self._headers,
            timeout=45_000,
        )
        headers_raw = getattr(response, "headers", {}) or {}
        if callable(headers_raw):
            maybe_headers = headers_raw()
            headers_raw = await maybe_headers if inspect.isawaitable(maybe_headers) else maybe_headers
        headers = {str(k).lower(): str(v) for k, v in dict(headers_raw).items()}
        try:
            maybe_json = response.json()
            raw_json = await maybe_json if inspect.isawaitable(maybe_json) else maybe_json
        except Exception:
            raw_text = ""
            try:
                maybe_text = response.text()
                raw_text = await maybe_text if inspect.isawaitable(maybe_text) else maybe_text
            except Exception:
                raw_text = ""
            raw_json = {"_raw_text": raw_text[:4000]} if isinstance(raw_text, str) and raw_text else {}
        return response.status, headers, raw_json

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
