from medium_stealth_bot.contracts import _live_read_operation_builders


def test_live_read_builder_requires_newsletter_username() -> None:
    builders, skipped = _live_read_operation_builders(
        tag_slug="programming",
        actor_user_id="cf6627889e92",
        newsletter_slug="d98ea047bd55",
        newsletter_username=None,
    )

    assert "NewsletterV3ViewerEdge" not in builders
    assert skipped["NewsletterV3ViewerEdge"] == "missing_newsletter_username"


def test_live_read_builder_includes_newsletter_username() -> None:
    builders, skipped = _live_read_operation_builders(
        tag_slug="programming",
        actor_user_id="cf6627889e92",
        newsletter_slug="d98ea047bd55",
        newsletter_username="thilo-hermann",
    )

    assert "NewsletterV3ViewerEdge" in builders
    assert "NewsletterV3ViewerEdge" not in skipped
    newsletter_op = builders["NewsletterV3ViewerEdge"]
    assert newsletter_op.variables["newsletterSlug"] == "d98ea047bd55"
    assert newsletter_op.variables["username"] == "thilo-hermann"
