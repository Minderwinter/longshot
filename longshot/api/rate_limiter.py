"""Thread-safe token bucket rate limiter."""

from __future__ import annotations

import threading
import time


class TokenBucket:
    """Token bucket rate limiter safe for use across multiple threads.

    Parameters
    ----------
    rate:
        Tokens added per second (sustained request rate).
    burst:
        Maximum tokens the bucket can hold (burst capacity).
    """

    def __init__(self, rate: float = 10.0, burst: float | None = None) -> None:
        self._rate = rate
        self._burst = burst if burst is not None else rate * 2
        self._tokens = self._burst
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Block until a token is available, then consume it."""
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
                self._last_refill = now

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return

            # Sleep briefly before retrying — outside the lock so other threads can proceed
            time.sleep(0.05)
