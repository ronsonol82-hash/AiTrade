# notifier.py
import aiohttp
import logging

log = logging.getLogger(__name__)

class TelegramAlerter:
    def __init__(self, bot_token: str, chat_id: str, enabled: bool = True):
        self.bot_token = bot_token or ""
        self.chat_id = chat_id or ""
        self.enabled = enabled and bool(self.bot_token and self.chat_id)
        self._session: aiohttp.ClientSession | None = None

    async def _ensure(self) -> None:
        if self.enabled and self._session is None:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def send(self, text: str) -> None:
        if not self.enabled:
            return
        await self._ensure()
        assert self._session is not None
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text, "disable_web_page_preview": True}
        try:
            async with self._session.post(url, json=payload) as resp:
                await resp.text()
        except Exception as e:
            log.warning(f"Telegram alert failed: {e}")
