"""Standalone ingestion pipeline for all non-MVE markets.

Fetches pages from Kalshi, buffers up to --chunk-size markets (default
100k), writes a numbered parquet chunk to S3, then moves on.

Usage:
    uv run python scripts/ingest_markets.py
    uv run python scripts/ingest_markets.py --chunk-size 50000
    uv run python scripts/ingest_markets.py --max-pages 3          # smoke test
"""

from __future__ import annotations

import argparse
import logging

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

from longshot.api.client import KalshiClient
from longshot.api.models import MarketsResponse
from longshot.api.rate_limiter import TokenBucket
from longshot.config import SETTINGS
from longshot.storage.s3 import MARKETS_SCHEMA, _markets_to_table

logger = logging.getLogger(__name__)


def _get_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=SETTINGS.aws_access_key_id,
        secret=SETTINGS.aws_secret_access_key,
        client_kwargs={"region_name": SETTINGS.aws_region},
    )


def _chunk_path(chunk_num: int) -> str:
    return (
        f"s3://{SETTINGS.s3_bucket}/{SETTINGS.s3_prefix}"
        f"/markets/all/chunk_{chunk_num:04d}.parquet"
    )


def write_chunk(fs: s3fs.S3FileSystem, table: pa.Table, chunk_num: int) -> str:
    path = _chunk_path(chunk_num)
    with fs.open(path, "wb") as f:
        pq.write_table(table, f)
    logger.info("Wrote chunk %d (%d rows) → %s", chunk_num, table.num_rows, path)
    return path


def run(chunk_size: int = 100_000, max_pages: int | None = None) -> None:
    limiter = TokenBucket(rate=10.0, burst=20.0)
    fs = _get_fs()

    buffer = []
    chunk_num = 0
    total_written = 0
    page = 0
    cursor: str | None = None

    with KalshiClient(limiter=limiter) as client:
        while True:
            params: dict = {"limit": 1000, "mve_filter": "exclude"}
            if cursor:
                params["cursor"] = cursor

            raw = client.get("/markets", params=params)
            resp = MarketsResponse.model_validate(raw)
            buffer.extend(resp.markets)
            page += 1

            logger.info(
                "Page %d: fetched %d (buffer: %d)",
                page,
                len(resp.markets),
                len(buffer),
            )

            # Flush buffer when it reaches chunk_size
            if len(buffer) >= chunk_size:
                table = _markets_to_table(buffer)
                write_chunk(fs, table, chunk_num)
                total_written += len(buffer)
                buffer = []
                chunk_num += 1

            # Stop conditions
            if not resp.cursor:
                break
            cursor = resp.cursor
            if max_pages and page >= max_pages:
                logger.info("Reached --max-pages %d, stopping", max_pages)
                break

    # Flush remaining
    if buffer:
        table = _markets_to_table(buffer)
        write_chunk(fs, table, chunk_num)
        total_written += len(buffer)
        chunk_num += 1

    print(f"\nDone: {total_written:,} markets across {chunk_num} chunk(s)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest all non-MVE markets from Kalshi into S3 (chunked)."
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100_000,
        help="Markets per parquet chunk (default: 100000)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Stop after N API pages (for smoke testing)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    run(chunk_size=args.chunk_size, max_pages=args.max_pages)


if __name__ == "__main__":
    main()
