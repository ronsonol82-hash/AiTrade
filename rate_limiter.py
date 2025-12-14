# rate_limiter.py
import asyncio
import time

class AsyncTokenBucket:
    def __init__(self, rate_per_sec: float, burst: int):
        self.rate = float(rate_per_sec)
        self.capacity = float(max(1, burst))
        self.tokens = self.capacity
        self.updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                dt = now - self.updated
                self.updated = now
                self.tokens = min(self.capacity, self.tokens + dt * self.rate)

                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return

                deficit = tokens - self.tokens
                wait_s = deficit / self.rate if self.rate > 0 else 1.0
            await asyncio.sleep(max(0.0, wait_s))
