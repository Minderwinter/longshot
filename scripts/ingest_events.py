"""Ingest all events from Kalshi into S3 as a single parquet file.

Events are small (just ticker, category, title, etc.) so no chunking needed.

Usage:
    uv run python scripts/ingest_events.py
"""

from __future__ import annotations

import logging

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

from longshot.api.client import KalshiClient
from longshot.api.rate_limiter import TokenBucket
from longshot.config import SETTINGS

logger = logging.getLogger(__name__)

EVENTS_SCHEMA = pa.schema(
    [
        pa.field("event_ticker", pa.string()),
        pa.field("category", pa.string()),
        pa.field("title", pa.string()),
        pa.field("sub_title", pa.string()),
        pa.field("mutually_exclusive", pa.bool_()),
        pa.field("series_ticker", pa.string()),
    ]
)


def _get_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=SETTINGS.aws_access_key_id,
        secret=SETTINGS.aws_secret_access_key,
        client_kwargs={"region_name": SETTINGS.aws_region},
    )


def _events_path() -> str:
    return f"s3://{SETTINGS.s3_bucket}/{SETTINGS.s3_prefix}/events/data.parquet"


def run() -> None:
    limiter = TokenBucket(rate=10.0, burst=20.0)

    events = []
    cursor: str | None = None
    page = 0

    with KalshiClient(limiter=limiter) as client:
        while True:
            params: dict = {"limit": 200}
            if cursor:
                params["cursor"] = cursor

            raw = client.get("/events", params=params)
            batch = raw["events"]
            events.extend(batch)
            page += 1

            logger.info("Page %d: fetched %d (total: %d)", page, len(batch), len(events))

            cursor = raw.get("cursor")
            if not cursor:
                break

    # Build table
    table = pa.table(
        {
            "event_ticker": [e["event_ticker"] for e in events],
            "category": [e.get("category") for e in events],
            "title": [e.get("title") for e in events],
            "sub_title": [e.get("sub_title") for e in events],
            "mutually_exclusive": [e.get("mutually_exclusive") for e in events],
            "series_ticker": [e.get("series_ticker") for e in events],
        },
        schema=EVENTS_SCHEMA,
    )

    # Write to S3
    fs = _get_fs()
    path = _events_path()
    with fs.open(path, "wb") as f:
        pq.write_table(table, f)

    print(f"\nDone: {len(events):,} events written to {path}")

    # Category summary
    cats = {}
    for e in events:
        cat = e.get("category", "Unknown")
        cats[cat] = cats.get(cat, 0) + 1
    print("\nCategories:")
    for cat, count in sorted(cats.items(), key=lambda x: -x[1]):
        print(f"  {cat}: {count:,}")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run()


if __name__ == "__main__":
    main()
