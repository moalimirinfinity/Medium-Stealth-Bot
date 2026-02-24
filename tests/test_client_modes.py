import asyncio

from medium_stealth_bot.client import MediumAsyncClient
from medium_stealth_bot.settings import AppSettings


class FakeAsyncSession:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.closed = False

    async def close(self):
        self.closed = True


class FakeBrowserContext:
    def __init__(self):
        self.request = object()
        self.closed = False
        self.cookies_added = []

    async def add_cookies(self, cookies):
        self.cookies_added.extend(cookies)

    async def cookies(self, url):
        return [{"name": "sid", "value": "ok"}]

    async def close(self):
        self.closed = True


class FakeChromium:
    async def launch_persistent_context(self, **kwargs):
        return FakeBrowserContext()


class FakePlaywright:
    def __init__(self):
        self.chromium = FakeChromium()

    async def stop(self):
        return None


class FakePlaywrightFactory:
    async def start(self):
        return FakePlaywright()


def test_client_fast_mode_smoke(monkeypatch):
    monkeypatch.setattr("medium_stealth_bot.client.curl_requests.AsyncSession", FakeAsyncSession)

    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION="sid=fake; uid=user; xsrf=csrf",
        MEDIUM_CSRF="csrf",
        CLIENT_MODE="fast",
    )
    client = MediumAsyncClient(settings)
    asyncio.run(client.open())
    assert client._session is not None
    asyncio.run(client.close())
    assert client._session is None


def test_client_stealth_mode_smoke(monkeypatch):
    monkeypatch.setattr("medium_stealth_bot.client.async_playwright", lambda: FakePlaywrightFactory())

    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION="sid=fake; uid=user; xsrf=csrf",
        MEDIUM_CSRF="csrf",
        CLIENT_MODE="stealth",
    )
    client = MediumAsyncClient(settings)
    asyncio.run(client.open())
    assert client._api_request is not None
    asyncio.run(client.close())
    assert client._api_request is None
