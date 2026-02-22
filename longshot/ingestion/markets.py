"""Paginated markets fetch from Kalshi API."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import datetime, timezone

from longshot.api.client import KalshiClient
from longshot.api.models import Market, MarketsResponse

logger = logging.getLogger(__name__)


def iter_all_markets(client: KalshiClient) -> Iterator[list[Market]]:
    """Yield pages of non-MVE markets from the API.

    Each yielded list is one page (up to 1000 markets). This avoids
    accumulating the entire universe in memory at once.
    """
    cursor: str | None = None
    page = 0
    total = 0

    while True:
        params: dict = {
            "limit": 1000,
            "mve_filter": "exclude",
        }
        if cursor:
            params["cursor"] = cursor

        raw = client.get("/markets", params=params)
        resp = MarketsResponse.model_validate(raw)
        page += 1
        total += len(resp.markets)
        logger.info(
            "Markets page %d: fetched %d (total so far: %d)",
            page,
            len(resp.markets),
            total,
        )

        yield resp.markets

        if not resp.cursor:
            break
        cursor = resp.cursor

    logger.info("Markets: %d total fetched (MVE excluded)", total)


def filter_markets_at_snapshot(
    markets: list[Market],
    snapshot_ts: int,
) -> list[Market]:
    """Return markets that were open at *snapshot_ts*.

    A market is considered open at the snapshot if:
      - created_time <= snapshot_ts  (it existed)
      - close_time   >  snapshot_ts  (it hadn't closed yet)
    """
    snapshot_dt = datetime.fromtimestamp(snapshot_ts, tz=timezone.utc)
    filtered = []

    for m in markets:
        # Parse created_time — include if missing (be permissive)
        if m.created_time:
            try:
                created = datetime.fromisoformat(m.created_time.replace("Z", "+00:00"))
                if created > snapshot_dt:
                    continue
            except (ValueError, TypeError):
                pass

        # Parse close_time — include if missing
        if m.close_time:
            try:
                closed = datetime.fromisoformat(m.close_time.replace("Z", "+00:00"))
                if closed <= snapshot_dt:
                    continue
            except (ValueError, TypeError):
                pass

        filtered.append(m)

    logger.info(
        "Snapshot filter (ts=%d): %d → %d markets",
        snapshot_ts,
        len(markets),
        len(filtered),
    )
    return filtered
