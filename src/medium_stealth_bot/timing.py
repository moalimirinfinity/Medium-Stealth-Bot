import asyncio
import random
import time

from medium_stealth_bot.settings import AppSettings


class HumanTimingController:
    """Centralized non-deterministic timing controls for runtime behavior."""

    def __init__(self, settings: AppSettings):
        self.settings = settings
        self._session_warmup_complete = False
        self._last_action_started_at: float | None = None

    async def maybe_sleep_session_warmup(self) -> float:
        if self._session_warmup_complete:
            return 0.0
        self._session_warmup_complete = True
        delay = self._sample_delay(
            low=float(self.settings.min_session_warmup_seconds),
            high=float(self.settings.max_session_warmup_seconds),
        )
        if delay > 0:
            await asyncio.sleep(delay)
        return delay

    async def sleep_read_delay(self) -> float:
        delay = self._sample_delay(
            low=float(self.settings.min_read_wait_seconds),
            high=float(self.settings.max_read_wait_seconds),
            mean=float(self.settings.pre_follow_read_wait_seconds),
        )
        if delay > 0:
            await asyncio.sleep(delay)
        return delay

    async def sleep_action_gap(self) -> float:
        now = time.monotonic()
        if self._last_action_started_at is None:
            self._last_action_started_at = now
            return 0.0

        required_gap = self._sample_delay(
            low=float(self.settings.min_action_gap_seconds),
            high=float(self.settings.max_action_gap_seconds),
        )
        elapsed = max(0.0, now - self._last_action_started_at)
        delay = max(0.0, required_gap - elapsed)
        if delay > 0:
            await asyncio.sleep(delay)
        self._last_action_started_at = time.monotonic()
        return delay

    @staticmethod
    def _sample_delay(*, low: float, high: float, mean: float | None = None) -> float:
        low = max(0.0, low)
        high = max(low, high)
        if high <= 0:
            return 0.0
        if high <= low:
            return low
        center = (low + high) / 2.0 if mean is None else max(low, min(high, mean))
        stddev = max((high - low) / 6.0, 0.1)
        sampled = random.gauss(center, stddev)
        return max(low, min(high, sampled))
