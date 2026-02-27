import asyncio
import random
import time
from collections import deque

from medium_stealth_bot.settings import AppSettings


class HumanTimingController:
    """Session-aware pacing controls for read and mutation behavior."""

    _ROLLING_MUTATION_WINDOW_SECONDS = 600.0

    def __init__(self, settings: AppSettings):
        self.settings = settings
        self._session_warmup_complete = False
        self._simulate_only = False
        self._simulated_monotonic_offset = 0.0
        self._last_action_started_at: float | None = None
        self._last_verify_started_at: float | None = None
        self._mutation_started_at: deque[float] = deque()
        self._actual_sleep_seconds_total = 0.0
        self._simulated_sleep_seconds_total = 0.0
        self._action_gap_seconds_total = 0.0
        self._read_delay_seconds_total = 0.0
        self._verify_gap_seconds_total = 0.0
        self._pass_cooldown_seconds_total = 0.0
        self._session_warmup_seconds_total = 0.0
        self._mutation_window_wait_seconds_total = 0.0
        self._mutation_window_limit_hits = 0

    @property
    def mutation_window_limit_hits(self) -> int:
        return self._mutation_window_limit_hits

    def set_simulation_mode(self, simulate_only: bool) -> None:
        self._simulate_only = bool(simulate_only)
        if not self._simulate_only:
            self._simulated_monotonic_offset = 0.0

    def reset_metrics(self) -> None:
        self._actual_sleep_seconds_total = 0.0
        self._simulated_sleep_seconds_total = 0.0
        self._action_gap_seconds_total = 0.0
        self._read_delay_seconds_total = 0.0
        self._verify_gap_seconds_total = 0.0
        self._pass_cooldown_seconds_total = 0.0
        self._session_warmup_seconds_total = 0.0
        self._mutation_window_wait_seconds_total = 0.0
        self._mutation_window_limit_hits = 0

    def reset_session_state(self) -> None:
        self._session_warmup_complete = False
        self._last_action_started_at = None
        self._last_verify_started_at = None
        self._mutation_started_at.clear()
        self._simulated_monotonic_offset = 0.0

    def metrics_snapshot(self) -> dict[str, float | int]:
        return {
            "timing_actual_sleep_seconds_total": round(self._actual_sleep_seconds_total, 3),
            "timing_simulated_sleep_seconds_total": round(self._simulated_sleep_seconds_total, 3),
            "timing_action_gap_seconds_total": round(self._action_gap_seconds_total, 3),
            "timing_verify_gap_seconds_total": round(self._verify_gap_seconds_total, 3),
            "timing_read_delay_seconds_total": round(self._read_delay_seconds_total, 3),
            "timing_pass_cooldown_seconds_total": round(self._pass_cooldown_seconds_total, 3),
            "timing_session_warmup_seconds_total": round(self._session_warmup_seconds_total, 3),
            "timing_mutation_window_wait_seconds_total": round(self._mutation_window_wait_seconds_total, 3),
            "timing_mutation_window_limit_hits": self._mutation_window_limit_hits,
        }

    async def maybe_sleep_session_warmup(self) -> float:
        if self._session_warmup_complete:
            return 0.0
        self._session_warmup_complete = True
        delay = self._sample_delay(
            low=float(self.settings.min_session_warmup_seconds),
            high=float(self.settings.max_session_warmup_seconds),
        )
        await self._wait(delay)
        self._session_warmup_seconds_total += delay
        return delay

    async def sleep_pass_cooldown(self) -> float:
        delay = self._sample_delay(
            low=float(self.settings.pass_cooldown_min_seconds),
            high=float(self.settings.pass_cooldown_max_seconds),
        )
        await self._wait(delay)
        self._pass_cooldown_seconds_total += delay
        return delay

    async def sleep_read_delay(self) -> float:
        delay = self._sample_delay(
            low=float(self.settings.min_read_wait_seconds),
            high=float(self.settings.max_read_wait_seconds),
            mean=float(self.settings.pre_follow_read_wait_seconds),
        )
        await self._wait(delay)
        self._read_delay_seconds_total += delay
        return delay

    async def sleep_verify_gap(self) -> float:
        now = self._now()
        if self._last_verify_started_at is None:
            self._last_verify_started_at = now
            return 0.0

        required_gap = self._sample_delay(
            low=float(self.settings.min_verify_gap_seconds),
            high=float(self.settings.max_verify_gap_seconds),
        )
        elapsed = max(0.0, now - self._last_verify_started_at)
        delay = max(0.0, required_gap - elapsed)
        await self._wait(delay)
        self._last_verify_started_at = self._now()
        self._verify_gap_seconds_total += delay
        return delay

    async def sleep_action_gap(self) -> float:
        now = self._now()
        required_gap = 0.0
        if self._last_action_started_at is not None:
            sampled_gap = self._sample_delay(
                low=float(self.settings.min_action_gap_seconds),
                high=float(self.settings.max_action_gap_seconds),
            )
            elapsed = max(0.0, now - self._last_action_started_at)
            required_gap = max(0.0, sampled_gap - elapsed)

        window_delay = self._mutation_window_delay(now=now)
        delay = max(required_gap, window_delay)
        await self._wait(delay)

        started = self._now()
        self._last_action_started_at = started
        self._mutation_started_at.append(started)
        self._prune_mutation_window(now=started)

        self._action_gap_seconds_total += delay
        if window_delay > 0:
            self._mutation_window_wait_seconds_total += window_delay
        return delay

    def _mutation_window_delay(self, *, now: float) -> float:
        cap = max(1, int(self.settings.max_mutations_per_10_minutes))
        self._prune_mutation_window(now=now)
        if len(self._mutation_started_at) < cap:
            return 0.0
        oldest = self._mutation_started_at[0]
        delay = max(0.0, self._ROLLING_MUTATION_WINDOW_SECONDS - (now - oldest))
        if delay > 0:
            self._mutation_window_limit_hits += 1
        return delay

    def _prune_mutation_window(self, *, now: float) -> None:
        cutoff = now - self._ROLLING_MUTATION_WINDOW_SECONDS
        while self._mutation_started_at and self._mutation_started_at[0] < cutoff:
            self._mutation_started_at.popleft()

    async def _wait(self, delay: float) -> None:
        if delay <= 0:
            return
        if self._simulate_only:
            self._simulated_sleep_seconds_total += delay
            self._simulated_monotonic_offset += delay
            return
        await asyncio.sleep(delay)
        self._actual_sleep_seconds_total += delay

    def _now(self) -> float:
        if self._simulate_only:
            return time.monotonic() + self._simulated_monotonic_offset
        return time.monotonic()

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
