from __future__ import annotations

import inspect
import json
from pathlib import Path
from typing import Any, Literal, Protocol

from curl_cffi import requests as curl_requests
from playwright.async_api import APIRequestContext, BrowserContext, Playwright, async_playwright

from medium_stealth_bot.browser_runtime import (
    build_medium_context_cookies,
    build_playwright_persistent_launch_kwargs,
)


class GraphQLTransport(Protocol):
    async def open(self) -> None:
        ...

    async def close(self) -> None:
        ...

    async def post_graphql(self, payload: list[dict[str, Any]]) -> tuple[int, dict[str, str], Any]:
        ...


class CurlCffiGraphQLTransport:
    def __init__(
        self,
        *,
        endpoint: str,
        impersonate: str,
        headers: dict[str, str],
        cookie_map: dict[str, str],
    ) -> None:
        self._endpoint = endpoint
        self._impersonate = impersonate
        self._headers = headers
        self._cookie_map = cookie_map
        self._session: curl_requests.AsyncSession | None = None

    async def open(self) -> None:
        if self._session is not None:
            return
        self._session = curl_requests.AsyncSession(
            impersonate=self._impersonate,
            headers=self._headers,
            cookies=self._cookie_map,
        )

    async def close(self) -> None:
        if self._session is None:
            return
        await self._session.close()
        self._session = None

    async def post_graphql(self, payload: list[dict[str, Any]]) -> tuple[int, dict[str, str], Any]:
        if self._session is None:
            raise RuntimeError("Fast client session is not initialized")

        response = await self._session.post(
            self._endpoint,
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


class PlaywrightGraphQLTransport:
    def __init__(
        self,
        *,
        profile_dir: Path,
        headless: bool,
        user_agent: str | None,
        channel: Literal["chrome", "chromium"] | None,
        endpoint: str,
        headers: dict[str, str],
        cookie_map: dict[str, str],
    ) -> None:
        self._profile_dir = profile_dir
        self._headless = headless
        self._user_agent = user_agent
        self._channel = channel
        self._endpoint = endpoint
        self._headers = headers
        self._cookie_map = cookie_map
        self._playwright: Playwright | None = None
        self._browser_context: BrowserContext | None = None
        self._api_request: APIRequestContext | None = None

    @staticmethod
    def _cookie_map_for_playwright(cookie_map: dict[str, str]) -> dict[str, str]:
        # Challenge cookies are volatile and can become stale quickly. Let the
        # browser profile own them instead of injecting from env/session strings.
        drop_names = {"cf_clearance", "_cfuvid"}
        return {name: value for name, value in cookie_map.items() if name not in drop_names}

    async def open(self) -> None:
        if self._api_request is not None:
            return
        try:
            self._playwright = await async_playwright().start()
            launch_kwargs = build_playwright_persistent_launch_kwargs(
                profile_dir=self._profile_dir,
                headless=self._headless,
                viewport={"width": 1280, "height": 800},
                user_agent=self._user_agent,
                channel=self._channel,
            )
            self._browser_context = await self._playwright.chromium.launch_persistent_context(**launch_kwargs)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                "Failed to launch Playwright persistent profile. "
                f"Profile path: {self._profile_dir}. "
                "If the profile is stale/corrupt, close running browsers and refresh via `uv run bot auth`."
            ) from exc

        context_cookie_map = self._cookie_map_for_playwright(self._cookie_map)
        if context_cookie_map:
            await self._browser_context.add_cookies(build_medium_context_cookies(context_cookie_map))
        stored_cookies = await self._browser_context.cookies("https://medium.com")
        has_sid = any(cookie.get("name") == "sid" for cookie in stored_cookies)
        if not has_sid:
            await self.close()
            raise RuntimeError(
                "No Medium `sid` cookie found in Playwright profile/session context. "
                "Run `uv run bot auth` to refresh login state."
            )
        self._api_request = self._browser_context.request

    async def close(self) -> None:
        if self._browser_context is not None:
            await self._browser_context.close()
            self._browser_context = None
        if self._playwright is not None:
            await self._playwright.stop()
            self._playwright = None
        self._api_request = None

    async def post_graphql(self, payload: list[dict[str, Any]]) -> tuple[int, dict[str, str], Any]:
        if self._api_request is None:
            raise RuntimeError("Stealth API context is not initialized")

        response = await self._api_request.post(
            self._endpoint,
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
