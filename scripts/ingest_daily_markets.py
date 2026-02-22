"""Daily pull of active (non-MVE) markets from Kalshi to S3.

Writes to: s3://{bucket}/{prefix}/markets/daily/date={YYYY-MM-DD}/hour={HH}/data.parquet

Only markets with close_time in the future are included.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone

import httpx
import pyarrow.parquet as pq
import s3fs

from longshot.api.client import KalshiClient
from longshot.api.models import MarketsResponse
from longshot.api.rate_limiter import TokenBucket
from longshot.config import SETTINGS
from longshot.storage.s3 import MARKETS_SCHEMA, _markets_to_table

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("daily_market_pull")


def main() -> None:
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    hour = now.hour
    close_ts = int(now.timestamp())
    s3_path = (
        f"s3://{SETTINGS.s3_bucket}/{SETTINGS.s3_prefix}"
        f"/markets/daily/date={date_str}/hour={hour:02d}/data.parquet"
    )

    logger.info("Daily market pull: date=%s hour=%02d min_close_ts=%d", date_str, hour, close_ts)
    logger.info("S3 path: %s", s3_path)

    limiter = TokenBucket(rate=10.0, burst=20.0)
    client = KalshiClient(limiter=limiter)

    # Try with mve_filter + min_close_ts, fall back if API rejects
    filter_used = "mve_filter=exclude + min_close_ts"
    params: dict = {"limit": 1000, "mve_filter": "exclude", "min_close_ts": close_ts}

    try:
        first_page_raw = client.get("/markets", params=params)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 400:
            logger.warning("400 with mve_filter + min_close_ts — falling back to min_close_ts only")
            filter_used = "min_close_ts only"
            params = {"limit": 1000, "min_close_ts": close_ts}
            first_page_raw = client.get("/markets", params=params)
        else:
            raise

    first_page = MarketsResponse.model_validate(first_page_raw)
    all_markets = list(first_page.markets)
    cursor = first_page.cursor
    pages = 1

    start = time.time()

    while cursor:
        params["cursor"] = cursor
        page_raw = client.get("/markets", params=params)
        page = MarketsResponse.model_validate(page_raw)
        all_markets.extend(page.markets)
        pages += 1
        cursor = page.cursor

        if pages % 10 == 0:
            logger.info("Page %d: %d markets so far", pages, len(all_markets))

    client.close()
    total_markets = len(all_markets)
    elapsed = time.time() - start

    # Write single parquet to S3
    fs = s3fs.S3FileSystem(
        key=SETTINGS.aws_access_key_id,
        secret=SETTINGS.aws_secret_access_key,
        client_kwargs={"region_name": SETTINGS.aws_region},
    )
    table = _markets_to_table(all_markets)
    with fs.open(s3_path, "wb") as f:
        pq.write_table(table, f)

    # File size
    s3_key = s3_path.replace("s3://", "")
    file_info = fs.info(s3_key)
    file_size_mb = file_info["size"] / (1024 * 1024)

    logger.info("Done: %d markets, %d pages, %.1fs", total_markets, pages, elapsed)
    logger.info("Filter: %s", filter_used)
    logger.info("File size: %.2f MB", file_size_mb)
    logger.info("S3 path: %s", s3_path)


if __name__ == "__main__":
    main()
