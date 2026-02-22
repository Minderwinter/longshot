"""HTTP client with RSA-PSS auth headers and retry logic for Kalshi API."""

from __future__ import annotations

import functools
import logging
import random
import time
from typing import Any
from urllib.parse import urlparse

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from longshot.api.rate_limiter import TokenBucket
from longshot.config import SETTINGS

logger = logging.getLogger(__name__)


def _sign(private_key_pem: str, timestamp_ms: int, method: str, path: str) -> str:
    """Generate RSA-PSS signature for Kalshi API auth.

    Signs: f"{timestamp_ms}{METHOD}{path_no_query}"
    """
    private_key = serialization.load_pem_private_key(
        private_key_pem.encode(), password=None
    )
    # path_no_query: strip query string
    path_no_query = path.split("?")[0]
    message = f"{timestamp_ms}{method}{path_no_query}".encode()
    signature = private_key.sign(  # type: ignore[union-attr]
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH,
        ),
        hashes.SHA256(),
    )
    import base64

    return base64.b64encode(signature).decode()


def retry(max_attempts: int = 5, base_delay: float = 1.0):
    """Decorator: exponential backoff with jitter on 429 / 5xx."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except httpx.HTTPStatusError as exc:
                    status = exc.response.status_code
                    if status == 429 or status >= 500:
                        if attempt == max_attempts:
                            raise
                        delay = base_delay * (2 ** (attempt - 1)) + random.uniform(
                            0, 0.5
                        )
                        logger.warning(
                            "HTTP %s on attempt %d/%d — retrying in %.1fs",
                            status,
                            attempt,
                            max_attempts,
                            delay,
                        )
                        time.sleep(delay)
                    else:
                        raise

        return wrapper

    return decorator


class KalshiClient:
    """Kalshi API client with connection pooling, auth, and rate limiting."""

    def __init__(self, limiter: TokenBucket | None = None) -> None:
        self._http = httpx.Client(
            base_url=SETTINGS.kalshi_base_url,
            timeout=30.0,
        )
        self._limiter = limiter

    def close(self) -> None:
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _auth_headers(self, method: str, path: str) -> dict[str, str]:
        ts_ms = int(time.time() * 1000)
        sig = _sign(SETTINGS.kalshi_private_key, ts_ms, method, path)
        return {
            "KALSHI-ACCESS-KEY": SETTINGS.kalshi_api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": str(ts_ms),
            "KALSHI-ACCESS-SIGNATURE": sig,
        }

    @retry()
    def get(self, path: str, params: dict[str, Any] | None = None) -> dict:
        """Authenticated GET request. Blocks on rate limiter before sending."""
        if self._limiter:
            self._limiter.acquire()
        headers = self._auth_headers("GET", path)
        resp = self._http.get(path, params=params, headers=headers)
        resp.raise_for_status()
        return resp.json()
