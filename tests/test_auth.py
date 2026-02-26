from pathlib import Path

import pytest

from medium_stealth_bot.auth import _auth_launch_kwargs, import_session_material_from_cookie_header
from medium_stealth_bot.settings import AppSettings


def test_auth_launch_kwargs_defaults_to_chrome_channel(tmp_path: Path) -> None:
    settings = AppSettings(_env_file=None)
    kwargs = _auth_launch_kwargs(settings, tmp_path / "profile")
    assert kwargs["channel"] == "chrome"
    assert kwargs["headless"] is False


def test_auth_launch_kwargs_omits_channel_for_chromium(tmp_path: Path) -> None:
    settings = AppSettings(_env_file=None, PLAYWRIGHT_AUTH_BROWSER_CHANNEL="chromium")
    kwargs = _auth_launch_kwargs(settings, tmp_path / "profile")
    assert "channel" not in kwargs
    assert kwargs["headless"] is False


def test_import_session_material_from_cookie_header() -> None:
    material = import_session_material_from_cookie_header("sid=session123; uid=user123; xsrf=csrf123")
    assert material.medium_session.startswith("sid=session123")
    assert material.medium_csrf == "csrf123"
    assert material.medium_user_ref == "user123"


def test_import_session_material_from_cookie_header_requires_sid() -> None:
    with pytest.raises(RuntimeError, match="sid cookie not found"):
        import_session_material_from_cookie_header("uid=user123; xsrf=csrf123")
