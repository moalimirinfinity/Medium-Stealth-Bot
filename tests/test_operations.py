from medium_stealth_bot.operations import USER_FOLLOWERS_MAX_LIMIT, user_followers


def test_user_followers_clamps_limit_to_supported_range() -> None:
    high = user_followers(user_id="abc123", limit=200)
    low = user_followers(user_id="abc123", limit=0)

    assert high.variables["paging"]["limit"] == USER_FOLLOWERS_MAX_LIMIT
    assert low.variables["paging"]["limit"] == 1
