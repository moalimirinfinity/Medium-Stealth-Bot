from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import Literal


def parse_cookie_header(cookie_header: str) -> dict[str, str]:
    cookies: dict[str, str] = {}
    for pair in cookie_header.split(";"):
        item = pair.strip()
        if not item or "=" not in item:
            continue
        key, value = item.split("=", 1)
        cookies[key.strip()] = value.strip()
    return cookies


def build_medium_context_cookies(cookie_map: dict[str, str]) -> list[dict[str, Any]]:
    return [
        {
            "name": name,
            "value": value,
            "domain": ".medium.com",
            "path": "/",
            "secure": True,
        }
        for name, value in cookie_map.items()
    ]


def build_playwright_persistent_launch_kwargs(
    *,
    profile_dir: Path,
    headless: bool,
    viewport: dict[str, int],
    user_agent: str | None,
    channel: Literal["chrome", "chromium"] | None,
) -> dict[str, Any]:
    launch_kwargs: dict[str, Any] = {
        "user_data_dir": str(profile_dir),
        "headless": headless,
        "viewport": viewport,
    }
    if user_agent:
        launch_kwargs["user_agent"] = user_agent
    if channel and channel != "chromium":
        launch_kwargs["channel"] = channel
    return launch_kwargs
