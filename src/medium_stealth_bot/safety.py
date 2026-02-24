from typing import Any

from medium_stealth_bot.models import GraphQLResult
from medium_stealth_bot.settings import AppSettings


class RiskHaltError(RuntimeError):
    def __init__(
        self,
        *,
        reason: str,
        task_name: str,
        detail: str,
        consecutive_failures: int,
    ) -> None:
        self.reason = reason
        self.task_name = task_name
        self.detail = detail
        self.consecutive_failures = consecutive_failures
        super().__init__(
            f"{reason} (task={task_name}, consecutive_failures={consecutive_failures}): {detail}"
        )


class RiskGuard:
    def __init__(self, settings: AppSettings, *, log):
        self.settings = settings
        self.log = log
        self._consecutive_failures = 0

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    def evaluate_result(
        self,
        *,
        task_name: str,
        result: GraphQLResult,
        is_final_attempt: bool,
    ) -> None:
        challenge_detail = self._detect_challenge(result)
        if challenge_detail:
            self._raise_halt(
                reason="challenge_detected",
                task_name=task_name,
                detail=challenge_detail,
            )

        session_detail = self._detect_session_expiry(result)
        if session_detail:
            self._raise_halt(
                reason="session_expiry_detected",
                task_name=task_name,
                detail=session_detail,
            )

        if not is_final_attempt:
            return

        if self._is_operation_failure(result):
            self._consecutive_failures += 1
            self.log.warning(
                "risk_failure_recorded",
                task_name=task_name,
                status_code=result.status_code,
                error_count=len(result.errors),
                consecutive_failures=self._consecutive_failures,
                halt_threshold=self.settings.risk_halt_consecutive_failures,
            )
            if self._consecutive_failures >= self.settings.risk_halt_consecutive_failures:
                self._raise_halt(
                    reason="consecutive_failure_threshold",
                    task_name=task_name,
                    detail=(
                        f"{self._consecutive_failures} consecutive operation failures "
                        f"(threshold={self.settings.risk_halt_consecutive_failures})"
                    ),
                )
            return

        if self._consecutive_failures > 0:
            self.log.info(
                "risk_failure_counter_reset",
                task_name=task_name,
                previous_consecutive_failures=self._consecutive_failures,
            )
        self._consecutive_failures = 0

    @staticmethod
    def _is_operation_failure(result: GraphQLResult) -> bool:
        return result.status_code != 200 or result.has_errors

    def _detect_challenge(self, result: GraphQLResult) -> str | None:
        if not self.settings.enable_challenge_halt:
            return None
        signal = self._signal_text(result)
        token = self._first_token_match(signal, self.settings.challenge_tokens)
        if token:
            return f"token='{token}', status={result.status_code}"
        if result.status_code in self.settings.challenge_status_codes and "<html" in signal:
            return f"status={result.status_code}, html_body_detected"
        return None

    def _detect_session_expiry(self, result: GraphQLResult) -> str | None:
        if not self.settings.enable_session_expiry_halt:
            return None
        if result.status_code in self.settings.session_expiry_status_codes:
            return f"status={result.status_code}"
        signal = self._signal_text(result)
        token = self._first_token_match(signal, self.settings.session_expiry_tokens)
        if token:
            return f"token='{token}', status={result.status_code}"
        return None

    @staticmethod
    def _first_token_match(signal: str, tokens: list[str]) -> str | None:
        for token in tokens:
            if token and token in signal:
                return token
        return None

    def _signal_text(self, result: GraphQLResult) -> str:
        chunks: list[str] = []
        for item in result.errors:
            if item.message:
                chunks.append(item.message)
        self._collect_raw_text(result.raw, chunks, depth=0)
        return " ".join(chunks).lower()

    def _collect_raw_text(self, raw: Any, chunks: list[str], *, depth: int) -> None:
        if depth > 3:
            return
        if isinstance(raw, str):
            if raw:
                chunks.append(raw)
            return
        if isinstance(raw, list):
            for item in raw[:6]:
                self._collect_raw_text(item, chunks, depth=depth + 1)
            return
        if not isinstance(raw, dict):
            return

        for key in ("message", "error", "detail", "title", "description", "_raw_text"):
            value = raw.get(key)
            if isinstance(value, str) and value:
                chunks.append(value)

        maybe_errors = raw.get("errors")
        if isinstance(maybe_errors, list):
            for item in maybe_errors[:6]:
                self._collect_raw_text(item, chunks, depth=depth + 1)

    def _raise_halt(self, *, reason: str, task_name: str, detail: str) -> None:
        self.log.error(
            "risk_halt_triggered",
            reason=reason,
            task_name=task_name,
            detail=detail,
            consecutive_failures=self._consecutive_failures,
        )
        raise RiskHaltError(
            reason=reason,
            task_name=task_name,
            detail=detail,
            consecutive_failures=self._consecutive_failures,
        )
