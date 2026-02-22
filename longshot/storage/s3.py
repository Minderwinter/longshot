"""PyArrow + s3fs parquet read/write with Hive-style partitioning."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

from collections.abc import Iterator

from longshot.api.models import Market, Trade
from longshot.config import SETTINGS

logger = logging.getLogger(__name__)

MARKETS_SCHEMA = pa.schema(
    [
        pa.field("ticker", pa.string()),
        pa.field("event_ticker", pa.string()),
        pa.field("title", pa.string()),
        pa.field("status", pa.string()),
        pa.field("yes_bid", pa.float64()),
        pa.field("yes_ask", pa.float64()),
        pa.field("no_bid", pa.float64()),
        pa.field("no_ask", pa.float64()),
        pa.field("last_price", pa.float64()),
        pa.field("volume", pa.int64()),
        pa.field("volume_24h", pa.int64()),
        pa.field("open_interest", pa.int64()),
        pa.field("close_time", pa.string()),
        pa.field("open_time", pa.string()),
        pa.field("result", pa.string()),
        pa.field("created_time", pa.string()),
    ]
)

TRADES_SCHEMA = pa.schema(
    [
        pa.field("trade_id", pa.string()),
        pa.field("ticker", pa.string()),
        pa.field("yes_price", pa.float64()),
        pa.field("no_price", pa.float64()),
        pa.field("count", pa.int64()),
        pa.field("taker_side", pa.string()),
        pa.field("created_time", pa.string()),
    ]
)


def _get_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=SETTINGS.aws_access_key_id,
        secret=SETTINGS.aws_secret_access_key,
        client_kwargs={"region_name": SETTINGS.aws_region},
    )


def _base_path() -> str:
    return f"s3://{SETTINGS.s3_bucket}/{SETTINGS.s3_prefix}"


def _snapshot_date_str(snapshot_ts: int) -> str:
    return datetime.fromtimestamp(snapshot_ts, tz=timezone.utc).strftime("%Y-%m-%d")


def _markets_to_table(markets: list[Market]) -> pa.Table:
    arrays = {
        "ticker": [m.ticker for m in markets],
        "event_ticker": [m.event_ticker for m in markets],
        "title": [m.title for m in markets],
        "status": [m.status for m in markets],
        "yes_bid": [m.yes_bid for m in markets],
        "yes_ask": [m.yes_ask for m in markets],
        "no_bid": [m.no_bid for m in markets],
        "no_ask": [m.no_ask for m in markets],
        "last_price": [m.last_price for m in markets],
        "volume": [m.volume for m in markets],
        "volume_24h": [m.volume_24h for m in markets],
        "open_interest": [m.open_interest for m in markets],
        "close_time": [m.close_time for m in markets],
        "open_time": [m.open_time for m in markets],
        "result": [m.result for m in markets],
        "created_time": [m.created_time for m in markets],
    }
    return pa.table(arrays, schema=MARKETS_SCHEMA)


# ---------------------------------------------------------------------------
# Full market universe (all non-MVE markets)
# ---------------------------------------------------------------------------

def _all_markets_path() -> str:
    return f"{_base_path()}/markets/all/data.parquet"


def stream_all_markets_parquet(
    pages: Iterator[list[Market]],
) -> tuple[str, int]:
    """Stream market pages into a parquet file on S3, one batch per page.

    Nothing accumulates in memory — each page is written and discarded.

    Returns ``(s3_path, total_written)``.
    """
    path = _all_markets_path()
    fs = _get_fs()
    total = 0

    with fs.open(path, "wb") as f:
        writer = pq.ParquetWriter(f, MARKETS_SCHEMA)
        for page in pages:
            batch = _markets_to_table(page)
            writer.write_table(batch)
            total += len(page)
        writer.close()

    logger.info("Wrote %d markets (full universe, streamed) to %s", total, path)
    return path, total


def read_all_markets() -> pa.Table:
    """Read the full non-MVE market universe from S3."""
    path = _all_markets_path()
    fs = _get_fs()
    with fs.open(path, "rb") as f:
        return pq.read_table(f)


# ---------------------------------------------------------------------------
# Snapshot-filtered markets
# ---------------------------------------------------------------------------

def _snapshot_markets_path(snapshot_date: str) -> str:
    return f"{_base_path()}/markets/snapshot_date={snapshot_date}/data.parquet"


def write_markets_parquet(markets: list[Market], snapshot_ts: int) -> str:
    """Write snapshot-filtered markets to S3. Returns the S3 path."""
    snapshot_date = _snapshot_date_str(snapshot_ts)
    path = _snapshot_markets_path(snapshot_date)
    table = _markets_to_table(markets)
    fs = _get_fs()
    with fs.open(path, "wb") as f:
        pq.write_table(table, f)
    logger.info("Wrote %d markets (snapshot) to %s", len(markets), path)
    return path


def read_markets(snapshot_date: str) -> pa.Table:
    """Read snapshot-filtered markets parquet from S3."""
    path = _snapshot_markets_path(snapshot_date)
    fs = _get_fs()
    with fs.open(path, "rb") as f:
        return pq.read_table(f)


# ---------------------------------------------------------------------------
# Trades
# ---------------------------------------------------------------------------

def _trades_path(snapshot_date: str) -> str:
    return f"{_base_path()}/trades/snapshot_date={snapshot_date}/data.parquet"


def write_trades_parquet(trades: list[Trade], snapshot_ts: int) -> str:
    """Write trades list to S3 as parquet. Returns the S3 path."""
    snapshot_date = _snapshot_date_str(snapshot_ts)
    path = _trades_path(snapshot_date)
    arrays = {
        "trade_id": [t.trade_id for t in trades],
        "ticker": [t.ticker for t in trades],
        "yes_price": [t.yes_price for t in trades],
        "no_price": [t.no_price for t in trades],
        "count": [t.count for t in trades],
        "taker_side": [t.taker_side for t in trades],
        "created_time": [t.created_time for t in trades],
    }
    table = pa.table(arrays, schema=TRADES_SCHEMA)
    fs = _get_fs()
    with fs.open(path, "wb") as f:
        pq.write_table(table, f)
    logger.info("Wrote %d trades to %s", len(trades), path)
    return path


def read_trades(snapshot_date: str) -> pa.Table:
    """Read trades parquet for a given snapshot date from S3."""
    path = _trades_path(snapshot_date)
    fs = _get_fs()
    with fs.open(path, "rb") as f:
        return pq.read_table(f)
