"""Threaded per-market trades fetch from Kalshi API."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from longshot.api.client import KalshiClient
from longshot.api.models import Trade, TradesResponse
from longshot.api.rate_limiter import TokenBucket

logger = logging.getLogger(__name__)


def fetch_trades_for_market(
    client: KalshiClient,
    ticker: str,
    max_ts: int,
    min_ts: int | None = None,
) -> list[Trade]:
    """Fetch all trades for a single market ticker between *min_ts* and *max_ts*."""
    trades: list[Trade] = []
    cursor: str | None = None

    while True:
        params: dict = {
            "ticker": ticker,
            "limit": 1000,
            "max_ts": max_ts,
        }
        if min_ts is not None:
            params["min_ts"] = min_ts
        if cursor:
            params["cursor"] = cursor

        try:
            raw = client.get("/markets/trades", params=params)
        except Exception:
            logger.exception("Failed to fetch trades for %s", ticker)
            return trades

        resp = TradesResponse.model_validate(raw)
        trades.extend(resp.trades)

        if not resp.cursor:
            break
        cursor = resp.cursor

    return trades


def fetch_all_trades(
    client: KalshiClient,
    limiter: TokenBucket,
    tickers: list[str],
    max_ts: int,
    min_ts: int | None = None,
    max_workers: int = 8,
) -> list[Trade]:
    """Fetch trades for all *tickers* in parallel using a thread pool.

    The shared ``TokenBucket`` on the client naturally serialises requests
    at the rate limit, so threads block when tokens are exhausted.
    """
    all_trades: list[Trade] = []
    failed: list[str] = []
    skipped = 0

    def _fetch(ticker: str) -> tuple[str, list[Trade]]:
        return ticker, fetch_trades_for_market(client, ticker, max_ts, min_ts=min_ts)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch, t): t for t in tickers}
        for i, future in enumerate(as_completed(futures), 1):
            ticker = futures[future]
            try:
                _, trades = future.result()
                if not trades:
                    skipped += 1
                all_trades.extend(trades)
                if i % 100 == 0 or i == len(tickers):
                    logger.info(
                        "Trades progress: %d/%d tickers (total trades: %d, skipped: %d)",
                        i,
                        len(tickers),
                        len(all_trades),
                        skipped,
                    )
            except Exception:
                logger.exception("Failed trades for %s", ticker)
                failed.append(ticker)

    if failed:
        logger.warning("Failed to fetch trades for %d tickers: %s", len(failed), failed[:20])

    logger.info(
        "Total trades fetched: %d across %d tickers (%d skipped/not found)",
        len(all_trades), len(tickers), skipped,
    )
    return all_trades
