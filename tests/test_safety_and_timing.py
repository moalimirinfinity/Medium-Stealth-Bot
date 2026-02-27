import asyncio
import time

import pytest
import structlog

from medium_stealth_bot.models import GraphQLError, GraphQLResult
from medium_stealth_bot.safety import RiskGuard, RiskHaltError
from medium_stealth_bot.settings import AppSettings
from medium_stealth_bot.timing import HumanTimingController


def _result(*, status: int, message: str = "") -> GraphQLResult:
    errors = [GraphQLError(message=message)] if message else []
    return GraphQLResult(
        operationName="TestOp",
        statusCode=status,
        data=None,
        errors=errors,
        raw={"message": message} if message else {},
    )


def test_risk_guard_halts_on_challenge_token() -> None:
    settings = AppSettings(
        _env_file=None,
        RISK_HALT_CONSECUTIVE_FAILURES=3,
        ENABLE_CHALLENGE_HALT=True,
        ENABLE_SESSION_EXPIRY_HALT=False,
    )
    guard = RiskGuard(settings=settings, log=structlog.get_logger("test"))

    with pytest.raises(RiskHaltError) as exc_info:
        guard.evaluate_result(
            task_name="mutation",
            result=_result(status=503, message="Just a moment"),
            is_final_attempt=True,
        )

    assert exc_info.value.reason == "challenge_detected"


def test_risk_guard_halts_on_consecutive_failures() -> None:
    settings = AppSettings(
        _env_file=None,
        RISK_HALT_CONSECUTIVE_FAILURES=2,
        ENABLE_CHALLENGE_HALT=False,
        ENABLE_SESSION_EXPIRY_HALT=False,
    )
    guard = RiskGuard(settings=settings, log=structlog.get_logger("test"))

    guard.evaluate_result(task_name="q1", result=_result(status=500), is_final_attempt=True)
    with pytest.raises(RiskHaltError) as exc_info:
        guard.evaluate_result(task_name="q2", result=_result(status=500), is_final_attempt=True)

    assert exc_info.value.reason == "consecutive_failure_threshold"


def test_risk_guard_halts_on_session_expiry_signal() -> None:
    settings = AppSettings(
        _env_file=None,
        ENABLE_CHALLENGE_HALT=False,
        ENABLE_SESSION_EXPIRY_HALT=True,
    )
    guard = RiskGuard(settings=settings, log=structlog.get_logger("test"))

    with pytest.raises(RiskHaltError) as exc_info:
        guard.evaluate_result(
            task_name="verify",
            result=_result(status=401, message="unauthorized"),
            is_final_attempt=True,
        )

    assert exc_info.value.reason == "session_expiry_detected"


def test_timing_controller_enforces_min_gap(monkeypatch) -> None:
    settings = AppSettings(
        _env_file=None,
        MIN_ACTION_GAP_SECONDS=10,
        MAX_ACTION_GAP_SECONDS=10,
    )
    controller = HumanTimingController(settings=settings)

    sleep_calls: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr("medium_stealth_bot.timing.asyncio.sleep", fake_sleep)
    monkeypatch.setattr(
        "medium_stealth_bot.timing.HumanTimingController._sample_delay",
        lambda *args, **kwargs: 10.0,
    )

    # First action starts baseline and does not sleep.
    first = asyncio.run(controller.sleep_action_gap())
    controller._last_action_started_at = time.monotonic() - 1.0
    # Second action only 1 second later, so sleeps 9 seconds.
    second = asyncio.run(controller.sleep_action_gap())

    assert first == 0.0
    assert 8.8 <= second <= 9.2
    assert len(sleep_calls) == 1


def test_timing_controller_enforces_verify_gap(monkeypatch) -> None:
    settings = AppSettings(
        _env_file=None,
        MIN_VERIFY_GAP_SECONDS=5,
        MAX_VERIFY_GAP_SECONDS=5,
    )
    controller = HumanTimingController(settings=settings)

    sleep_calls: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr("medium_stealth_bot.timing.asyncio.sleep", fake_sleep)
    monkeypatch.setattr(
        "medium_stealth_bot.timing.HumanTimingController._sample_delay",
        lambda *args, **kwargs: 5.0,
    )

    first = asyncio.run(controller.sleep_verify_gap())
    controller._last_verify_started_at = controller._now() - 1.0
    second = asyncio.run(controller.sleep_verify_gap())

    assert first == 0.0
    assert 3.8 <= second <= 4.2
    assert len(sleep_calls) == 1


def test_timing_controller_enforces_mutation_window_limit(monkeypatch) -> None:
    settings = AppSettings(
        _env_file=None,
        MIN_ACTION_GAP_SECONDS=0,
        MAX_ACTION_GAP_SECONDS=0,
        MAX_MUTATIONS_PER_10_MINUTES=1,
    )
    controller = HumanTimingController(settings=settings)

    sleep_calls: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr("medium_stealth_bot.timing.asyncio.sleep", fake_sleep)
    monkeypatch.setattr(
        "medium_stealth_bot.timing.HumanTimingController._sample_delay",
        lambda *args, **kwargs: 0.0,
    )

    first = asyncio.run(controller.sleep_action_gap())
    second = asyncio.run(controller.sleep_action_gap())

    assert first == 0.0
    assert second >= 590.0
    assert controller.mutation_window_limit_hits >= 1
    assert len(sleep_calls) == 1


def test_timing_controller_simulation_mode_avoids_real_sleep(monkeypatch) -> None:
    settings = AppSettings(
        _env_file=None,
        MIN_ACTION_GAP_SECONDS=10,
        MAX_ACTION_GAP_SECONDS=10,
    )
    controller = HumanTimingController(settings=settings)
    controller.set_simulation_mode(True)

    async def fail_sleep(delay: float) -> None:  # pragma: no cover - assertion path
        raise AssertionError(f"sleep should not be called in simulation mode (delay={delay})")

    monkeypatch.setattr("medium_stealth_bot.timing.asyncio.sleep", fail_sleep)
    monkeypatch.setattr(
        "medium_stealth_bot.timing.HumanTimingController._sample_delay",
        lambda *args, **kwargs: 10.0,
    )

    first = asyncio.run(controller.sleep_action_gap())
    controller._last_action_started_at = controller._now() - 1.0
    second = asyncio.run(controller.sleep_action_gap())
    metrics = controller.metrics_snapshot()

    assert first == 0.0
    assert 8.8 <= second <= 9.2
    assert metrics["timing_simulated_sleep_seconds_total"] >= second
