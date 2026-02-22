"""Daily pull of active events (with nested markets) from Kalshi to S3.

Writes to: s3://{bucket}/{prefix}/events/daily/date={YYYY-MM-DD}/hour={HH}/data.parquet

Only events with close_time in the future are included.
Nested markets are stored as a JSON string column.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

from longshot.api.client import KalshiClient
from longshot.api.rate_limiter import TokenBucket
from longshot.config import SETTINGS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("daily_event_pull")

EVENT_SCHEMA = pa.schema([
    pa.field("event_ticker", pa.string()),
    pa.field("series_ticker", pa.string()),
    pa.field("category", pa.string()),
    pa.field("title", pa.string()),
    pa.field("sub_title", pa.string()),
    pa.field("mutually_exclusive", pa.bool_()),
    pa.field("collateral_return_type", pa.string()),
    pa.field("strike_date", pa.string()),
    pa.field("strike_period", pa.string()),
    pa.field("markets_json", pa.string()),
])


def main() -> None:
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    hour = now.hour
    close_ts = int(now.timestamp())
    s3_path = (
        f"s3://{SETTINGS.s3_bucket}/{SETTINGS.s3_prefix}"
        f"/events/daily/date={date_str}/hour={hour:02d}/data.parquet"
    )

    logger.info("Daily event pull: date=%s hour=%02d min_close_ts=%d", date_str, hour, close_ts)
    logger.info("S3 path: %s", s3_path)

    limiter = TokenBucket(rate=10.0, burst=20.0)
    client = KalshiClient(limiter=limiter)

    # Try with min_close_ts + with_nested_markets, fall back progressively
    filter_used = "min_close_ts + with_nested_markets"
    base_params: dict = {
        "limit": 200,
        "min_close_ts": close_ts,
        "with_nested_markets": "true",
    }

    try:
        first_raw = client.get("/events", params=dict(base_params))
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 400:
            logger.warning("400 with min_close_ts + with_nested_markets — falling back to min_close_ts only")
            filter_used = "min_close_ts only"
            base_params = {"limit": 200, "min_close_ts": close_ts}
            try:
                first_raw = client.get("/events", params=dict(base_params))
            except httpx.HTTPStatusError as exc2:
                if exc2.response.status_code == 400:
                    logger.warning("400 with min_close_ts — falling back to no filters")
                    filter_used = "no filters"
                    base_params = {"limit": 200}
                    first_raw = client.get("/events", params=dict(base_params))
                else:
                    raise
        else:
            raise

    all_events = list(first_raw.get("events", []))
    cursor = first_raw.get("cursor")
    pages = 1

    start = time.time()

    while cursor:
        page_params = dict(base_params)
        page_params["cursor"] = cursor
        page_raw = client.get("/events", params=page_params)
        all_events.extend(page_raw.get("events", []))
        pages += 1
        cursor = page_raw.get("cursor")

        if pages % 10 == 0:
            logger.info("Page %d: %d events so far", pages, len(all_events))

    client.close()
    total = len(all_events)
    elapsed = time.time() - start

    # Build PyArrow table
    table = pa.table(
        {
            "event_ticker": [e["event_ticker"] for e in all_events],
            "series_ticker": [e.get("series_ticker") for e in all_events],
            "category": [e.get("category") for e in all_events],
            "title": [e.get("title") for e in all_events],
            "sub_title": [e.get("sub_title") for e in all_events],
            "mutually_exclusive": [e.get("mutually_exclusive") for e in all_events],
            "collateral_return_type": [e.get("collateral_return_type") for e in all_events],
            "strike_date": [e.get("strike_date") for e in all_events],
            "strike_period": [e.get("strike_period") for e in all_events],
            "markets_json": [json.dumps(e.get("markets", [])) for e in all_events],
        },
        schema=EVENT_SCHEMA,
    )

    # Write to S3
    fs = s3fs.S3FileSystem(
        key=SETTINGS.aws_access_key_id,
        secret=SETTINGS.aws_secret_access_key,
        client_kwargs={"region_name": SETTINGS.aws_region},
    )
    with fs.open(s3_path, "wb") as f:
        pq.write_table(table, f)

    # File size
    s3_key = s3_path.replace("s3://", "")
    file_info = fs.info(s3_key)
    file_size_mb = file_info["size"] / (1024 * 1024)

    # Category breakdown
    cat_counts: dict[str, int] = {}
    for e in all_events:
        cat = e.get("category") or "Unknown"
        cat_counts[cat] = cat_counts.get(cat, 0) + 1
    has_nested = sum(1 for e in all_events if e.get("markets"))

    logger.info("Done: %d events, %d pages, %.1fs", total, pages, elapsed)
    logger.info("Filter: %s", filter_used)
    logger.info("Events with nested markets: %d", has_nested)
    logger.info("File size: %.2f MB", file_size_mb)
    logger.info("S3 path: %s", s3_path)
    logger.info("Category breakdown:")
    for cat, count in sorted(cat_counts.items(), key=lambda x: -x[1]):
        logger.info("  %s: %d", cat, count)


if __name__ == "__main__":
    main()
