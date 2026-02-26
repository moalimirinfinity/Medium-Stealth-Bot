from medium_stealth_bot.settings import AppSettings


def test_settings_builds_medium_session_from_split_cookie_values() -> None:
    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION_SID="sid_value",
        MEDIUM_SESSION_UID="uid_value",
        MEDIUM_SESSION_XSRF="xsrf_value",
    )

    assert settings.medium_session == "sid=sid_value; uid=uid_value; xsrf=xsrf_value"
    assert settings.medium_csrf == "xsrf_value"
    assert settings.medium_user_ref == "uid_value"
    assert settings.has_session is True


def test_settings_prefers_medium_session_when_provided() -> None:
    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION="sid=header_sid; uid=header_uid",
        MEDIUM_SESSION_SID="sid_split",
        MEDIUM_SESSION_UID="uid_split",
    )

    assert settings.medium_session == "sid=header_sid; uid=header_uid"
