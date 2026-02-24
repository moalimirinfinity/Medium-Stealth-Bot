import asyncio
from pathlib import Path
from typing import Any

import structlog
from playwright.async_api import async_playwright

from medium_stealth_bot.models import AuthSessionMaterial
from medium_stealth_bot.settings import AppSettings

COOKIE_ORDER = ("sid", "uid", "xsrf", "cf_clearance", "_cfuvid")


def _serialize_cookie_map(cookie_map: dict[str, str]) -> str:
    ordered_names = [name for name in COOKIE_ORDER if name in cookie_map]
    ordered_names.extend(sorted(name for name in cookie_map if name not in ordered_names))
    return "; ".join(f"{name}={cookie_map[name]}" for name in ordered_names)


def _quote_env(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def extract_session_material(cookies: list[dict[str, Any]]) -> AuthSessionMaterial:
    medium_cookies = [
        cookie
        for cookie in cookies
        if "medium.com" in str(cookie.get("domain", "")).lower()
    ]
    cookie_map = {
        str(cookie.get("name")): str(cookie.get("value"))
        for cookie in medium_cookies
        if cookie.get("name") and cookie.get("value")
    }

    if "sid" not in cookie_map:
        raise RuntimeError("sid cookie not found; login likely incomplete.")

    return AuthSessionMaterial(
        MEDIUM_SESSION=_serialize_cookie_map(cookie_map),
        MEDIUM_CSRF=cookie_map.get("xsrf") or cookie_map.get("XSRF-TOKEN"),
        MEDIUM_USER_REF=cookie_map.get("uid"),
        cookie_names=sorted(cookie_map.keys()),
    )


async def interactive_auth(settings: AppSettings, login_url: str = "https://medium.com/m/signin") -> AuthSessionMaterial:
    log = structlog.get_logger(__name__)
    settings.ensure_directories()
    profile_dir = settings.playwright_profile_dir
    profile_dir.mkdir(parents=True, exist_ok=True)

    log.info("auth_start", profile_dir=str(profile_dir), login_url=login_url)
    async with async_playwright() as playwright:
        context = await playwright.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=False,
            viewport={"width": 1366, "height": 900},
        )
        try:
            page = context.pages[0] if context.pages else await context.new_page()
            await page.goto(login_url, wait_until="domcontentloaded")
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                lambda: input(
                    "Complete Medium login in the browser, then press Enter here to capture cookies..."
                ),
            )
            cookies = await context.cookies("https://medium.com")
            material = extract_session_material(cookies)
            log.info("auth_captured", cookie_count=len(material.cookie_names))
            return material
        finally:
            await context.close()


def upsert_env_file(env_path: Path, material: AuthSessionMaterial) -> None:
    env_path.parent.mkdir(parents=True, exist_ok=True)
    existing_lines = env_path.read_text(encoding="utf-8").splitlines() if env_path.exists() else []

    updates = {
        "MEDIUM_SESSION": material.medium_session,
        "MEDIUM_CSRF": material.medium_csrf,
        "MEDIUM_USER_REF": material.medium_user_ref,
    }

    written_keys: set[str] = set()
    output_lines: list[str] = []
    for line in existing_lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            output_lines.append(line)
            continue
        key = line.split("=", 1)[0].strip()
        if key not in updates:
            output_lines.append(line)
            continue
        value = updates[key]
        if value is None:
            output_lines.append(f"{key}=")
        else:
            output_lines.append(f"{key}={_quote_env(value)}")
        written_keys.add(key)

    for key, value in updates.items():
        if key in written_keys or value is None:
            continue
        output_lines.append(f"{key}={_quote_env(value)}")

    env_path.write_text("\n".join(output_lines).rstrip() + "\n", encoding="utf-8")
