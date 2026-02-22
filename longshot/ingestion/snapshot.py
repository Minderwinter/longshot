"""Top-level orchestrator: fetch markets + trades for a snapshot timestamp."""

from __future__ import annotations

import logging

from longshot.api.client import KalshiClient
from longshot.api.models import Market
from longshot.api.rate_limiter import TokenBucket
from longshot.ingestion.markets import iter_all_markets, filter_markets_at_snapshot
from longshot.ingestion.trades import fetch_all_trades
from longshot.storage.s3 import (
    read_all_markets,
    stream_all_markets_parquet,
    write_markets_parquet,
    write_trades_parquet,
)

logger = logging.getLogger(__name__)


def run_snapshot(snapshot_ts: int, *, skip_trades: bool = False) -> dict:
    """Run a full snapshot ingestion for the given Unix timestamp.

    1. Stream all non-MVE markets to S3 (page by page, constant memory)
    2. Read back from S3, filter for snapshot window, write snapshot file
    3. (unless *skip_trades*) Fetch trades in parallel → write to S3

    Returns summary dict with counts and S3 paths.
    """
    limiter = TokenBucket(rate=10.0, burst=20.0)

    with KalshiClient(limiter=limiter) as client:
        # --- Stream all markets to S3 ---
        logger.info("Fetching all non-MVE markets (streaming to S3) ...")
        all_markets_path, all_count = stream_all_markets_parquet(
            iter_all_markets(client),
        )
        logger.info("Full universe: %d markets → %s", all_count, all_markets_path)

        # --- Read back and filter for snapshot ---
        logger.info("Reading back markets and filtering for snapshot ...")
        all_table = read_all_markets()
        all_markets = [Market.model_validate(row) for row in all_table.to_pylist()]
        snapshot_markets = filter_markets_at_snapshot(all_markets, snapshot_ts)
        snapshot_markets_path = write_markets_parquet(snapshot_markets, snapshot_ts)
        logger.info(
            "Snapshot: %d markets → %s", len(snapshot_markets), snapshot_markets_path
        )

        # --- Trades ---
        trades_path = None
        trade_count = 0
        if not skip_trades:
            tickers = [m.ticker for m in snapshot_markets]
            logger.info("Fetching trades for %d tickers ...", len(tickers))
            trades = fetch_all_trades(client, limiter, tickers, max_ts=snapshot_ts)
            trades_path = write_trades_parquet(trades, snapshot_ts)
            trade_count = len(trades)
            logger.info("Trades: %d → %s", trade_count, trades_path)
        else:
            logger.info("Skipping trades fetch")

    summary = {
        "snapshot_ts": snapshot_ts,
        "all_market_count": all_count,
        "snapshot_market_count": len(snapshot_markets),
        "trade_count": trade_count,
        "all_markets_path": all_markets_path,
        "snapshot_markets_path": snapshot_markets_path,
        "trades_path": trades_path,
    }
    logger.info("Snapshot complete: %s", summary)
    return summary
