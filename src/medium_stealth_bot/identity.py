from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Mapping

from medium_stealth_bot.settings import AppSettings


@dataclass(frozen=True)
class BrowserIdentityProfile:
    user_agent: str
    graphql_origin: str
    graphql_referer: str
    apollo_client_name: str
    apollo_client_version: str
    curl_impersonate: str
    playwright_channel: Literal["chrome", "chromium"]
    accept_language: str | None

    def graphql_headers(self, *, csrf_token: str | None) -> dict[str, str]:
        headers = {
            "Origin": self.graphql_origin,
            "Referer": self.graphql_referer,
            "User-Agent": self.user_agent,
            "Content-Type": "application/json",
            "apollographql-client-name": self.apollo_client_name,
            "apollographql-client-version": self.apollo_client_version,
        }
        if self.accept_language:
            headers["Accept-Language"] = self.accept_language
        if csrf_token:
            headers["x-xsrf-token"] = csrf_token
        return headers


def resolve_browser_identity(settings: AppSettings) -> BrowserIdentityProfile:
    return BrowserIdentityProfile(
        user_agent=settings.user_agent,
        graphql_origin=settings.graphql_origin,
        graphql_referer=settings.graphql_referer,
        apollo_client_name=settings.apollo_client_name,
        apollo_client_version=settings.apollo_client_version,
        curl_impersonate=settings.curl_impersonate,
        playwright_channel=settings.playwright_auth_browser_channel,
        accept_language=settings.http_accept_language or None,
    )


def resolve_csrf_token(settings: AppSettings, cookie_map: Mapping[str, str]) -> str | None:
    return settings.medium_csrf or cookie_map.get("xsrf") or cookie_map.get("XSRF-TOKEN")
