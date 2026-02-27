from medium_stealth_bot.models import GraphQLResult
from medium_stealth_bot.typed_payloads import parse_user_followers_next_from


def _followers_result(next_from: str | None) -> GraphQLResult:
    paging = {"next": {"from": next_from}} if next_from is not None else {"next": None}
    return GraphQLResult(
        operationName="UserFollowers",
        statusCode=200,
        data={
            "userResult": {
                "followersUserConnection": {
                    "users": [],
                    "pagingInfo": paging,
                }
            }
        },
        errors=[],
        raw={},
    )


def test_parse_user_followers_next_from_returns_cursor() -> None:
    result = _followers_result("cursor_2")
    assert parse_user_followers_next_from(result) == "cursor_2"


def test_parse_user_followers_next_from_handles_blank_or_missing() -> None:
    blank = _followers_result("   ")
    missing = _followers_result(None)
    no_paging = GraphQLResult(
        operationName="UserFollowers",
        statusCode=200,
        data={"userResult": {"followersUserConnection": {"users": []}}},
        errors=[],
        raw={},
    )
    assert parse_user_followers_next_from(blank) is None
    assert parse_user_followers_next_from(missing) is None
    assert parse_user_followers_next_from(no_paging) is None
