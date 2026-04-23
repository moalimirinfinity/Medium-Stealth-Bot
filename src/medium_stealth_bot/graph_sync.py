from __future__ import annotations

import asyncio
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import structlog
from playwright.async_api import BrowserContext, Page, Response, async_playwright

from medium_stealth_bot import operations
from medium_stealth_bot.browser_runtime import (
    build_medium_context_cookies,
    build_playwright_persistent_launch_kwargs,
    parse_cookie_header,
)
from medium_stealth_bot.client import MediumAsyncClient
from medium_stealth_bot.identity import resolve_browser_identity
from medium_stealth_bot.contract_registry import load_operation_contract_registry
from medium_stealth_bot.models import GraphQLError, GraphQLOperation, GraphSyncOutcome, GraphQLResult
from medium_stealth_bot.repository import ActionRepository
from medium_stealth_bot.settings import AppSettings
from medium_stealth_bot.typed_payloads import UserNode, parse_user_followers_next_from, parse_user_followers_users

_FOLLOWING_QUERY = """
query UserFollowing($username: ID, $id: ID, $paging: PagingOptions) {
  userResult(username: $username, id: $id) {
    __typename
    ... on User {
      id
      followingUserConnection(paging: $paging) {
        users {
          id
          name
          username
          bio
          socialStats {
            followerCount
            followingCount
          }
          newsletterV3 {
            id
          }
        }
        pagingInfo {
          next {
            from
            limit
          }
        }
      }
    }
  }
}
""".strip()


class GraphSyncService:
    def __init__(self, *, settings: AppSettings, client: MediumAsyncClient, repository: ActionRepository):
        self.settings = settings
        self.client = client
        self.repository = repository
        self.log = structlog.get_logger(__name__)

    async def sync(
        self,
        *,
        dry_run: bool,
        mode: str,
        force: bool = False,
    ) -> GraphSyncOutcome:
        started = time.perf_counter()

        if mode == "auto" and not self.settings.graph_sync_auto_enabled and not force:
            return GraphSyncOutcome(
                dry_run=dry_run,
                mode=mode,
                skipped=True,
                skip_reason="auto_sync_disabled",
            )

        if mode == "auto" and not force and self._is_recent_success():
            return GraphSyncOutcome(
                dry_run=dry_run,
                mode=mode,
                skipped=True,
                skip_reason="fresh_cache_window",
            )

        run_id = None if dry_run else self.repository.begin_graph_sync_run(
            mode=mode,
            source_path=str(self.settings.implementation_ops_registry_path),
        )
        followers_count = 0
        following_count = 0
        users_upserted_count = 0
        imported_pending_count = 0
        followers_source = "unknown"
        following_source = "unknown"
        error_message: str | None = None
        status = "success"

        try:
            followers_rows, followers_source = await self._fetch_followers_rows()
            if dry_run:
                followers_count = len(followers_rows)
            else:
                followers_count = self.repository.replace_own_followers_snapshot(followers_rows, run_id=run_id)

            following_rows, following_source = await self._fetch_following_rows()
            if dry_run:
                following_count = len(following_rows)
            else:
                following_count = self.repository.replace_own_following_snapshot(following_rows, run_id=run_id)
                users_upserted_count = self.repository.upsert_users_from_social_caches()
                imported_pending_count = self.repository.upsert_imported_follow_cycle_pending_from_following_cache()
        except Exception as exc:  # noqa: BLE001
            status = "failed"
            error_message = str(exc)
            self.log.warning(
                "graph_sync_failed",
                mode=mode,
                error=error_message,
            )
        finally:
            if run_id is not None:
                self.repository.complete_graph_sync_run(
                    run_id,
                    status=status,
                    followers_count=followers_count,
                    following_count=following_count,
                    imported_pending_count=imported_pending_count,
                    error_message=error_message,
                )

        duration_ms = int((time.perf_counter() - started) * 1000)
        if status != "success":
            raise RuntimeError(error_message or "graph sync failed")
        self.log.info(
            "graph_sync_complete",
            mode=mode,
            run_id=run_id,
            followers_count=followers_count,
            following_count=following_count,
            followers_source=followers_source,
            users_upserted_count=users_upserted_count,
            imported_pending_count=imported_pending_count,
            following_source=following_source,
            duration_ms=duration_ms,
        )
        return GraphSyncOutcome(
            dry_run=dry_run,
            mode=mode,
            run_id=run_id,
            skipped=False,
            followers_count=followers_count,
            following_count=following_count,
            users_upserted_count=users_upserted_count,
            imported_pending_count=imported_pending_count,
            source_path=str(self.settings.implementation_ops_registry_path),
            used_following_source=following_source,
            duration_ms=duration_ms,
        )

    async def _fetch_followers_rows(self) -> tuple[list[dict[str, object]], str]:
        try:
            rows = await self._fetch_followers_rows_graphql()
            if rows:
                return rows, "graphql"
            self.log.warning("graph_sync_followers_graphql_empty")
        except Exception as exc:  # noqa: BLE001
            self.log.warning(
                "graph_sync_followers_graphql_fallback",
                error_type=type(exc).__name__,
                error=self._summarize_exception(exc),
            )

        cached_ids = sorted(self.repository.cached_own_follower_ids())
        if cached_ids:
            self.log.warning(
                "graph_sync_followers_using_cached_snapshot",
                cached_count=len(cached_ids),
            )
            return [{"user_id": user_id} for user_id in cached_ids], "cached"

        self.log.warning("graph_sync_followers_unavailable_no_cache")
        return [], "unavailable"

    def _is_recent_success(self) -> bool:
        freshness_minutes = max(0, self.settings.graph_sync_freshness_window_minutes)
        if freshness_minutes <= 0:
            return False
        value = self.repository.latest_graph_sync_success_at()
        if not value:
            return False
        parsed = self._parse_sqlite_utc_datetime(value)
        if parsed is None:
            return False
        age = datetime.now(timezone.utc) - parsed
        return age <= timedelta(minutes=freshness_minutes)

    @staticmethod
    def _parse_sqlite_utc_datetime(value: str) -> datetime | None:
        text = value.strip()
        if not text:
            return None
        normalized = text.replace(" ", "T")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    async def _fetch_followers_rows_graphql(self) -> list[dict[str, object]]:
        actor_user_id = self.settings.medium_user_ref
        if not actor_user_id:
            return []

        per_page_limit = max(1, min(operations.USER_FOLLOWERS_MAX_LIMIT, self.settings.own_followers_scan_limit))
        cursor: str | None = None
        seen_cursors: set[str] = set()
        collected: dict[str, dict[str, object]] = {}

        while True:
            operation = operations.user_followers(
                user_id=actor_user_id,
                limit=per_page_limit,
                paging_from=cursor,
            )
            result = await self._execute_graphql_with_retry(
                operation=operation,
                task_name="graph_sync_followers_graphql",
            )
            self._assert_result_ok(result, task_name="graph_sync_followers_graphql")
            for node in parse_user_followers_users(result):
                row = self._user_node_to_row(node)
                if row is not None:
                    collected[row["user_id"]] = row

            next_cursor = parse_user_followers_next_from(result)
            if not self.settings.graph_sync_full_pagination:
                break
            if not next_cursor or next_cursor in seen_cursors:
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor
            await asyncio.sleep(self._graph_sync_pagination_delay_seconds())

        return list(collected.values())

    async def _fetch_following_rows(self) -> tuple[list[dict[str, object]], str]:
        if self.settings.graph_sync_enable_graphql_following:
            for operation_name in self._following_operation_candidates():
                try:
                    rows, complete = await self._fetch_following_rows_graphql(operation_name=operation_name)
                    if rows and not complete:
                        rows = self._merge_rows_with_cached_ids(rows, self.repository.cached_own_following_ids())
                    if rows:
                        return rows, "graphql"
                    self.log.warning(
                        "graph_sync_following_graphql_empty",
                        operation_name=operation_name,
                    )
                except Exception as exc:  # noqa: BLE001
                    self.log.warning(
                        "graph_sync_following_graphql_fallback",
                        operation_name=operation_name,
                        error_type=type(exc).__name__,
                        error=self._summarize_exception(exc),
                    )
        if not self.settings.graph_sync_enable_scrape_fallback:
            raise RuntimeError("no_following_source_available")
        max_attempts = 2
        rows: list[dict[str, object]] = []
        for attempt in range(1, max_attempts + 1):
            rows = await self._fetch_following_rows_scrape()
            if rows:
                cached_ids = self.repository.cached_own_following_ids()
                if cached_ids and len(rows) < len(cached_ids):
                    rows = self._merge_rows_with_cached_ids(rows, cached_ids)
                return rows, "scrape"
            self.log.warning(
                "graph_sync_following_scrape_empty",
                attempt=attempt,
                max_attempts=max_attempts,
            )

        cached_ids = sorted(self.repository.cached_own_following_ids())
        if cached_ids:
            self.log.warning(
                "graph_sync_following_scrape_empty_using_cached_snapshot",
                cached_count=len(cached_ids),
            )
            return [{"user_id": user_id} for user_id in cached_ids], "scrape_cached"
        return rows, "scrape"

    def _following_operation_candidates(self) -> list[str]:
        # Prefer operation names present in the local registry, but always keep
        # stable defaults so following sync can run even when the registry lags.
        candidates = ["UserFollowing", "UserFollowingQuery"]
        path = Path(self.settings.implementation_ops_registry_path)
        if not path.is_absolute():
            path = (Path(__file__).resolve().parents[2] / path).resolve()
        if not path.exists():
            return candidates
        try:
            registry = load_operation_contract_registry(path=path, strict=self.settings.contract_registry_strict)
        except Exception:
            return candidates

        ordered: list[str] = []
        available = registry.registry.operation_map()
        for name in candidates:
            if name in available:
                ordered.append(name)
        for name in candidates:
            if name not in ordered:
                ordered.append(name)
        return ordered

    async def _fetch_following_rows_graphql(self, *, operation_name: str) -> tuple[list[dict[str, object]], bool]:
        actor_user_id = self.settings.medium_user_ref
        if not actor_user_id:
            return [], True

        per_page_limit = max(1, min(operations.USER_FOLLOWERS_MAX_LIMIT, self.settings.own_followers_scan_limit))
        cursor: str | None = None
        seen_cursors: set[str] = set()
        collected: dict[str, dict[str, object]] = {}
        complete = True
        registry = getattr(self.client, "_contract_registry", None)
        restore_strict: bool | None = None
        if (
            registry is not None
            and bool(getattr(registry, "strict", True))
            and operation_name not in registry.registry.operation_map()
        ):
            restore_strict = True
            registry.strict = False
            self.log.warning(
                "graph_sync_unregistered_operation_non_strict",
                operation_name=operation_name,
            )
        try:
            while True:
                operation = GraphQLOperation(
                    operationName=operation_name,
                    query=_FOLLOWING_QUERY,
                    variables={
                        "id": actor_user_id,
                        "username": None,
                        "paging": {
                            "limit": per_page_limit,
                            "from": cursor or "",
                        },
                    },
                )
                try:
                    result = await self._execute_graphql_with_retry(
                        operation=operation,
                        task_name="graph_sync_following_graphql",
                    )
                    self._assert_result_ok(result, task_name="graph_sync_following_graphql")
                except Exception as exc:
                    if collected:
                        complete = False
                        self.log.warning(
                            "graph_sync_following_graphql_partial_return",
                            operation_name=operation_name,
                            collected_count=len(collected),
                            error_type=type(exc).__name__,
                            error=self._summarize_exception(exc),
                        )
                        break
                    raise

                users, next_cursor = self._parse_following_users_and_next(result)
                for payload in users:
                    row = self._row_from_payload_dict(payload)
                    if row is not None:
                        collected[row["user_id"]] = row

                if not self.settings.graph_sync_full_pagination:
                    break
                if not next_cursor or next_cursor in seen_cursors:
                    break
                seen_cursors.add(next_cursor)
                cursor = next_cursor
                await asyncio.sleep(self._graph_sync_pagination_delay_seconds())
        finally:
            if restore_strict and registry is not None:
                registry.strict = True

        return list(collected.values()), complete

    async def _execute_graphql_with_retry(
        self,
        *,
        operation: GraphQLOperation,
        task_name: str,
        max_attempts: int = 5,
    ) -> GraphQLResult:
        retryable_statuses = {0, 408, 425, 429, 500, 502, 503, 504}
        attempt = 1
        while True:
            try:
                result = await self.client.execute(operation)
            except Exception as exc:  # noqa: BLE001
                error_text = self._summarize_exception(exc, max_length=400)
                result = GraphQLResult(
                    operationName=operation.operation_name,
                    statusCode=0,
                    data=None,
                    errors=[GraphQLError(message=error_text)],
                    raw={
                        "exception_type": type(exc).__name__,
                        "exception": error_text,
                    },
                )
            if result.status_code == 200 and not result.has_errors:
                return result

            retryable = result.status_code in retryable_statuses
            if not retryable or attempt >= max_attempts:
                return result

            delay_seconds = min(16.0, float(2 ** (attempt - 1)))
            self.log.warning(
                "graph_sync_retry_scheduled",
                task_name=task_name,
                operation_name=operation.operation_name,
                attempt=attempt,
                max_attempts=max_attempts,
                status_code=result.status_code,
                error_count=len(result.errors),
                delay_seconds=delay_seconds,
            )
            await asyncio.sleep(delay_seconds)
            attempt += 1

    @staticmethod
    def _graph_sync_pagination_delay_seconds() -> float:
        # Keep page fetches conservative to reduce 429 bursts on larger follow graphs.
        return random.uniform(0.8, 1.4)

    def _merge_rows_with_cached_ids(
        self,
        rows: list[dict[str, object]],
        cached_ids: set[str],
    ) -> list[dict[str, object]]:
        merged: dict[str, dict[str, object]] = {}
        for row in rows:
            user_id = str(row.get("user_id") or "").strip()
            if not user_id:
                continue
            merged[user_id] = row
        for user_id in sorted(cached_ids):
            if user_id not in merged:
                merged[user_id] = {"user_id": user_id}
        if cached_ids and len(merged) > len(rows):
            self.log.warning(
                "graph_sync_following_graphql_merged_with_cache",
                graphql_count=len(rows),
                cached_count=len(cached_ids),
                merged_count=len(merged),
            )
        return list(merged.values())

    @staticmethod
    def _summarize_exception(exc: Exception, *, max_length: int = 240) -> str:
        normalized = " ".join(str(exc).split())
        if "Call log:" in normalized:
            normalized = normalized.split("Call log:", 1)[0].strip()
        if not normalized:
            return type(exc).__name__
        if len(normalized) > max_length:
            return f"{normalized[: max_length - 3]}..."
        return normalized

    @staticmethod
    def _parse_following_users_and_next(result: GraphQLResult) -> tuple[list[dict[str, Any]], str | None]:
        data = result.data or {}
        user_result = data.get("userResult") if isinstance(data, dict) else None
        if not isinstance(user_result, dict):
            return [], None
        connection = user_result.get("followingUserConnection")
        if not isinstance(connection, dict):
            return [], None
        users_raw = connection.get("users")
        users = [item for item in users_raw if isinstance(item, dict)] if isinstance(users_raw, list) else []
        paging_info = connection.get("pagingInfo")
        next_obj = paging_info.get("next") if isinstance(paging_info, dict) else None
        next_cursor = next_obj.get("from") if isinstance(next_obj, dict) else None
        if isinstance(next_cursor, str):
            next_cursor = next_cursor.strip() or None
        else:
            next_cursor = None
        return users, next_cursor

    async def _fetch_following_rows_scrape(self) -> list[dict[str, object]]:
        users_by_id: dict[str, dict[str, object]] = {}
        response_tasks: list[asyncio.Task[None]] = []
        timeout_ms = int(self.settings.graph_sync_scrape_page_timeout_seconds * 1000)
        identity = resolve_browser_identity(self.settings)

        async with async_playwright() as playwright:
            launch_kwargs = build_playwright_persistent_launch_kwargs(
                profile_dir=self.settings.playwright_profile_dir,
                headless=self.settings.playwright_headless,
                viewport={"width": 1280, "height": 800},
                user_agent=identity.user_agent,
                channel=identity.playwright_channel,
            )
            context: BrowserContext
            try:
                context = await playwright.chromium.launch_persistent_context(**launch_kwargs)
            except Exception:
                launch_kwargs.pop("channel", None)
                context = await playwright.chromium.launch_persistent_context(**launch_kwargs)
            try:
                cookie_map = parse_cookie_header(self.settings.medium_session or "")
                if cookie_map:
                    await context.add_cookies(build_medium_context_cookies(cookie_map))

                page: Page = context.pages[0] if context.pages else await context.new_page()

                def _handle_response(response: Response) -> None:
                    response_tasks.append(
                        asyncio.create_task(
                            self._capture_users_from_graphql_response(response, users_by_id)
                        )
                    )

                page.on("response", _handle_response)
                await page.goto("https://medium.com/me/following", wait_until="domcontentloaded", timeout=timeout_ms)
                await self._scrape_prime_page(page)

                stable_rounds = 0
                last_count = len(users_by_id)
                max_rounds = 80 if self.settings.graph_sync_full_pagination else 12
                for round_index in range(max_rounds):
                    await self._scrape_scroll_step(
                        page=page,
                        round_index=round_index,
                        stable_rounds=stable_rounds,
                    )
                    count = len(users_by_id)
                    if count <= last_count:
                        stable_rounds += 1
                    else:
                        stable_rounds = 0
                        last_count = count
                    if stable_rounds >= 4:
                        break

                if response_tasks:
                    await asyncio.gather(*response_tasks, return_exceptions=True)
            finally:
                await context.close()

        return list(users_by_id.values())

    async def _scrape_prime_page(self, page: Page) -> None:
        await page.wait_for_timeout(random.randint(600, 1300))
        await self._maybe_move_mouse(page)

    async def _scrape_scroll_step(
        self,
        *,
        page: Page,
        round_index: int,
        stable_rounds: int,
    ) -> None:
        await self._maybe_move_mouse(page)
        await page.mouse.wheel(0, self._sample_scroll_delta_px(stable_rounds=stable_rounds))
        await page.wait_for_timeout(
            self._sample_scroll_pause_ms(
                stable_rounds=stable_rounds,
                long_pause_bias=(round_index % 5 == 4),
            )
        )

        # Small burst and reverse jitter keeps scrolling less mechanical.
        if random.random() < 0.28:
            await page.mouse.wheel(0, random.randint(500, 3200))
            await page.wait_for_timeout(random.randint(120, 380))
        if random.random() < 0.14:
            await page.mouse.wheel(0, -random.randint(180, 950))
            await page.wait_for_timeout(random.randint(180, 460))

    async def _maybe_move_mouse(self, page: Page) -> None:
        if random.random() > 0.45:
            return
        viewport = page.viewport_size or {"width": 1280, "height": 800}
        width = max(320, int(viewport.get("width", 1280)))
        height = max(320, int(viewport.get("height", 800)))
        target_x = int(width * random.uniform(0.2, 0.8))
        target_y = int(height * random.uniform(0.2, 0.85))
        steps = random.randint(7, 20)
        try:
            await page.mouse.move(target_x, target_y, steps=steps)
        except Exception:
            return

    @staticmethod
    def _sample_scroll_delta_px(*, stable_rounds: int) -> int:
        base = random.randint(2200, 6800)
        if stable_rounds >= 1:
            base += random.randint(600, 1800)
        if stable_rounds >= 3:
            base += random.randint(900, 2600)
        if random.random() < 0.12:
            base += random.randint(1400, 3600)
        return max(700, min(14000, base))

    @staticmethod
    def _sample_scroll_pause_ms(*, stable_rounds: int, long_pause_bias: bool) -> int:
        if stable_rounds >= 3:
            pause = random.randint(850, 1650)
        elif stable_rounds >= 1:
            pause = random.randint(520, 1180)
        else:
            pause = random.randint(280, 860)
        if long_pause_bias:
            pause += random.randint(120, 360)
        return min(2200, pause)

    async def _capture_users_from_graphql_response(
        self,
        response: Response,
        users_by_id: dict[str, dict[str, object]],
    ) -> None:
        if "/_/graphql" not in response.url:
            return
        try:
            payload = await response.json()
        except Exception:
            return
        items = payload if isinstance(payload, list) else [payload]
        for item in items:
            if not isinstance(item, dict):
                continue
            data = item.get("data")
            if not isinstance(data, dict):
                continue
            for row in self._extract_user_rows_from_payload(data):
                users_by_id[row["user_id"]] = row

    def _extract_user_rows_from_payload(self, data: dict[str, Any]) -> list[dict[str, object]]:
        rows: dict[str, dict[str, object]] = {}
        stack: list[Any] = [data]
        while stack:
            current = stack.pop()
            if isinstance(current, dict):
                row = self._row_from_payload_dict(current)
                if row is not None:
                    rows[row["user_id"]] = row
                stack.extend(current.values())
            elif isinstance(current, list):
                stack.extend(current)
        return list(rows.values())

    def _row_from_payload_dict(self, payload: dict[str, Any]) -> dict[str, object] | None:
        raw_id = payload.get("id")
        if not isinstance(raw_id, str):
            return None
        user_id = raw_id.strip()
        if not user_id:
            return None
        is_userish = any(key in payload for key in ("username", "bio", "socialStats", "newsletterV3"))
        if not is_userish:
            return None
        username = payload.get("username")
        name = payload.get("name")
        bio = payload.get("bio")
        social_stats = payload.get("socialStats")
        follower_count = social_stats.get("followerCount") if isinstance(social_stats, dict) else None
        following_count = social_stats.get("followingCount") if isinstance(social_stats, dict) else None
        newsletter_v3 = payload.get("newsletterV3")
        newsletter_v3_id = newsletter_v3.get("id") if isinstance(newsletter_v3, dict) else None
        return {
            "user_id": user_id,
            "username": username if isinstance(username, str) else None,
            "name": name if isinstance(name, str) else None,
            "bio": bio if isinstance(bio, str) else None,
            "follower_count": follower_count if isinstance(follower_count, int) else None,
            "following_count": following_count if isinstance(following_count, int) else None,
            "newsletter_v3_id": newsletter_v3_id if isinstance(newsletter_v3_id, str) else None,
        }

    def _user_node_to_row(self, node: UserNode) -> dict[str, object] | None:
        user_id = node.id.strip() if isinstance(node.id, str) else ""
        if not user_id:
            return None
        follower_count = node.social_stats.follower_count if node.social_stats else None
        following_count = node.social_stats.following_count if node.social_stats else None
        newsletter_v3_id = node.newsletter_v3.id if node.newsletter_v3 else None
        return {
            "user_id": user_id,
            "username": node.username,
            "name": node.name,
            "bio": node.bio,
            "follower_count": follower_count,
            "following_count": following_count,
            "newsletter_v3_id": newsletter_v3_id,
        }

    @staticmethod
    def _assert_result_ok(result: GraphQLResult, *, task_name: str) -> None:
        if result.status_code == 200 and not result.has_errors:
            return
        error_messages = [item.message.strip() for item in result.errors if item.message.strip()]
        detail = ""
        if error_messages:
            preview = "; ".join(error_messages[:2])
            if len(error_messages) > 2:
                preview = f"{preview} (+{len(error_messages) - 2} more)"
            detail = f" message={preview!r}"
        raise RuntimeError(
            f"{task_name} failed: status={result.status_code} errors={len(result.errors)}{detail}"
        )
