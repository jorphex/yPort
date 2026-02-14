import aiohttp
from typing import Optional


class SharedHttpClient:
    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self) -> None:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self._session = aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            raise RuntimeError("HTTP session not started")
        return self._session
