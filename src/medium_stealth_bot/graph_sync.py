from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import structlog
from playwright.async_api import BrowserContext, Page, Response, async_playwright

from medium_stealth_bot import operations
from medium_stealth_bot.client import MediumAsyncClient, parse_cookie_header
from medium_stealth_bot.contract_registry import load_operation_contract_registry
from medium_stealth_bot.models import GraphQLOperation, GraphSyncOutcome, GraphQLResult
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

        if not force and self._is_recent_success():
            return GraphSyncOutcome(
                dry_run=dry_run,
                mode=mode,
                skipped=True,
                skip_reason="fresh_cache_window",
            )

        run_id = self.repository.begin_graph_sync_run(
            mode=mode,
            source_path=str(self.settings.implementation_ops_registry_path),
        )
        followers_count = 0
        following_count = 0
        imported_pending_count = 0
        following_source = "unknown"
        error_message: str | None = None
        status = "success"

        try:
            followers_rows = await self._fetch_followers_rows_graphql()
            followers_count = self.repository.replace_own_followers_snapshot(followers_rows, run_id=run_id)

            following_rows, following_source = await self._fetch_following_rows()
            following_count = self.repository.replace_own_following_snapshot(following_rows, run_id=run_id)

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
            imported_pending_count=imported_pending_count,
            source_path=str(self.settings.implementation_ops_registry_path),
            used_following_source=following_source,
            duration_ms=duration_ms,
        )

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

        per_page_limit = max(1, min(500, self.settings.own_followers_scan_limit))
        cursor: str | None = None
        seen_cursors: set[str] = set()
        collected: dict[str, dict[str, object]] = {}

        while True:
            result = await self.client.execute(
                operations.user_followers(
                    user_id=actor_user_id,
                    limit=per_page_limit,
                    paging_from=cursor,
                )
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

        return list(collected.values())

    async def _fetch_following_rows(self) -> tuple[list[dict[str, object]], str]:
        if self.settings.graph_sync_enable_graphql_following:
            operation_name = self._supported_graphql_following_operation_name()
            if operation_name:
                try:
                    rows = await self._fetch_following_rows_graphql(operation_name=operation_name)
                    return rows, "graphql"
                except Exception as exc:  # noqa: BLE001
                    self.log.warning(
                        "graph_sync_following_graphql_fallback",
                        operation_name=operation_name,
                        error=str(exc),
                    )
        if not self.settings.graph_sync_enable_scrape_fallback:
            raise RuntimeError("no_following_source_available")
        rows = await self._fetch_following_rows_scrape()
        return rows, "scrape"

    def _supported_graphql_following_operation_name(self) -> str | None:
        path = Path(self.settings.implementation_ops_registry_path)
        if not path.is_absolute():
            path = (Path(__file__).resolve().parents[2] / path).resolve()
        if not path.exists():
            return None
        try:
            registry = load_operation_contract_registry(path=path, strict=self.settings.contract_registry_strict)
        except Exception:
            return None
        candidates = ("UserFollowing", "UserFollowingQuery")
        available = registry.registry.operation_map()
        for name in candidates:
            if name in available:
                return name
        return None

    async def _fetch_following_rows_graphql(self, *, operation_name: str) -> list[dict[str, object]]:
        actor_user_id = self.settings.medium_user_ref
        if not actor_user_id:
            return []

        per_page_limit = max(1, min(500, self.settings.own_followers_scan_limit))
        cursor: str | None = None
        seen_cursors: set[str] = set()
        collected: dict[str, dict[str, object]] = {}

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
            result = await self.client.execute(operation)
            self._assert_result_ok(result, task_name="graph_sync_following_graphql")

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

        return list(collected.values())

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

        async with async_playwright() as playwright:
            launch_kwargs: dict[str, object] = {
                "user_data_dir": str(self.settings.playwright_profile_dir),
                "headless": self.settings.playwright_headless,
                "viewport": {"width": 1280, "height": 800},
                "user_agent": self.settings.user_agent,
            }
            if self.settings.playwright_auth_browser_channel != "chromium":
                launch_kwargs["channel"] = self.settings.playwright_auth_browser_channel
            context: BrowserContext
            try:
                context = await playwright.chromium.launch_persistent_context(**launch_kwargs)
            except Exception:
                launch_kwargs.pop("channel", None)
                context = await playwright.chromium.launch_persistent_context(**launch_kwargs)
            try:
                cookie_map = parse_cookie_header(self.settings.medium_session or "")
                if cookie_map:
                    await context.add_cookies(
                        [
                            {
                                "name": key,
                                "value": value,
                                "domain": ".medium.com",
                                "path": "/",
                                "secure": True,
                            }
                            for key, value in cookie_map.items()
                        ]
                    )

                page: Page = context.pages[0] if context.pages else await context.new_page()

                def _handle_response(response: Response) -> None:
                    response_tasks.append(
                        asyncio.create_task(
                            self._capture_users_from_graphql_response(response, users_by_id)
                        )
                    )

                page.on("response", _handle_response)
                await page.goto("https://medium.com/me/following", wait_until="domcontentloaded", timeout=timeout_ms)
                await page.wait_for_timeout(1000)

                stable_rounds = 0
                last_count = len(users_by_id)
                max_rounds = 80 if self.settings.graph_sync_full_pagination else 12
                for _ in range(max_rounds):
                    await page.mouse.wheel(0, 18000)
                    await page.wait_for_timeout(700)
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
        raise RuntimeError(
            f"{task_name} failed: status={result.status_code} errors={len(result.errors)}"
        )
