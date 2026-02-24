import asyncio
import time
from datetime import datetime, timezone

import structlog

from medium_stealth_bot.client import MediumAsyncClient
from medium_stealth_bot.models import DailyRunOutcome, ProbeSnapshot
from medium_stealth_bot import operations
from medium_stealth_bot.repository import ActionRepository
from medium_stealth_bot.settings import AppSettings


class DailyRunner:
    def __init__(self, settings: AppSettings, client: MediumAsyncClient, repository: ActionRepository):
        self.settings = settings
        self.client = client
        self.repository = repository
        self.log = structlog.get_logger(__name__)

    async def probe(self, tag_slug: str = "programming") -> ProbeSnapshot:
        started = datetime.now(timezone.utc)
        start_time = time.perf_counter()

        task_map: dict[str, asyncio.Task] = {
            "base_cache": asyncio.create_task(self.client.execute(operations.use_base_cache_control())),
            "topic_latest_stories": asyncio.create_task(self.client.execute(operations.topic_latest_stories(tag_slug))),
            "topic_who_to_follow": asyncio.create_task(
                self.client.execute(operations.topic_who_to_follow_publishers(tag_slug=tag_slug))
            ),
            "who_to_follow_module": asyncio.create_task(self.client.execute(operations.who_to_follow_module())),
        }

        # Run an independent viewer-edge check in parallel when MEDIUM_USER_REF is a user id.
        if self.settings.medium_user_ref and not self.settings.medium_user_ref.startswith("@"):
            task_map["user_viewer_edge"] = asyncio.create_task(
                self.client.execute(operations.user_viewer_edge(self.settings.medium_user_ref))
            )

        resolved = await asyncio.gather(*task_map.values())
        results = dict(zip(task_map.keys(), resolved, strict=True))

        duration_ms = int((time.perf_counter() - start_time) * 1000)
        self.log.info("probe_complete", tag_slug=tag_slug, duration_ms=duration_ms, task_count=len(task_map))
        return ProbeSnapshot(
            tag_slug=tag_slug,
            started_at=started,
            duration_ms=duration_ms,
            results=results,
        )

    async def run_daily_cycle(self, tag_slug: str = "programming") -> DailyRunOutcome:
        actions_today = self.repository.actions_today()
        max_actions = self.settings.max_actions_per_day
        if actions_today >= max_actions:
            self.log.info("budget_exhausted", actions_today=actions_today, max_actions=max_actions)
            return DailyRunOutcome(
                budget_exhausted=True,
                actions_today=actions_today,
                max_actions_per_day=max_actions,
                probe=None,
            )

        probe = await self.probe(tag_slug=tag_slug)
        self.log.info("daily_cycle_complete", actions_today=actions_today, max_actions=max_actions)
        return DailyRunOutcome(
            budget_exhausted=False,
            actions_today=actions_today,
            max_actions_per_day=max_actions,
            probe=probe,
        )
