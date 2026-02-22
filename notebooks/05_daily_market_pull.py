# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "marimo",
#     "httpx",
#     "pyarrow",
#     "s3fs",
#     "python-dotenv",
#     "pandas",
# ]
# ///

import marimo

__generated_with = "0.10.0"
app = marimo.App(width="medium")


@app.cell
def setup_and_imports():
    import marimo as mo
    import time
    import logging
    from datetime import datetime, timezone

    import httpx
    import pyarrow as pa
    import pyarrow.parquet as pq
    import s3fs
    import pandas as pd

    from longshot.api.client import KalshiClient
    from longshot.api.rate_limiter import TokenBucket
    from longshot.api.models import MarketsResponse
    from longshot.storage.s3 import MARKETS_SCHEMA, _markets_to_table
    from longshot.config import SETTINGS

    logging.basicConfig(level=logging.INFO)
    pull_logger = logging.getLogger("daily_pull")

    return (
        MARKETS_SCHEMA, SETTINGS, MarketsResponse, TokenBucket,
        KalshiClient, _markets_to_table, datetime, httpx, mo, pa, pd,
        pq, pull_logger, s3fs, time, timezone,
    )


@app.cell
def pull_params(SETTINGS, datetime, mo, timezone):
    pull_now = datetime.now(timezone.utc)
    pull_date_str = pull_now.strftime("%Y-%m-%d")
    pull_hour = pull_now.hour
    pull_close_ts = int(pull_now.timestamp())
    pull_s3_path = (
        f"s3://{SETTINGS.s3_bucket}/{SETTINGS.s3_prefix}"
        f"/markets/daily/date={pull_date_str}/hour={pull_hour:02d}/data.parquet"
    )

    mo.md(
        f"""
        # Daily Active Market Pull

        Pull all non-MVE markets from Kalshi with `close_time` in the future
        and write to S3.

        | Parameter | Value |
        |-----------|-------|
        | Timestamp (UTC) | `{pull_now.isoformat()}` |
        | Date | `{pull_date_str}` |
        | Hour | `{pull_hour:02d}` |
        | min_close_ts | `{pull_close_ts}` |
        | S3 path | `{pull_s3_path}` |
        """
    )

    return pull_close_ts, pull_date_str, pull_hour, pull_now, pull_s3_path


@app.cell
def pull_and_write_markets(
    KalshiClient, MARKETS_SCHEMA, MarketsResponse, SETTINGS, TokenBucket,
    _markets_to_table, httpx, mo, pq, pull_close_ts, pull_logger, pull_s3_path,
    s3fs, time,
):
    mo.md("## Pulling markets from Kalshi API...")

    BATCH_SIZE = 10_000
    limiter = TokenBucket(rate=10.0, burst=20.0)
    client = KalshiClient(limiter=limiter)

    # Use mve_filter + min_close_ts to get non-MVE markets closing in the future
    pull_filter_used = "mve_filter=exclude + min_close_ts"
    params = {"limit": 1000, "mve_filter": "exclude", "min_close_ts": pull_close_ts}

    try:
        first_page_raw = client.get("/markets", params=params)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 400:
            pull_logger.warning(
                "400 with mve_filter + min_close_ts — falling back to min_close_ts only"
            )
            pull_filter_used = "min_close_ts only"
            params = {"limit": 1000, "min_close_ts": pull_close_ts}
            first_page_raw = client.get("/markets", params=params)
        else:
            raise

    # Parse first page
    first_page = MarketsResponse.model_validate(first_page_raw)
    all_markets = list(first_page.markets)
    cursor = first_page.cursor
    pull_pages = 1

    pull_start = time.time()

    # Paginate remaining pages — accumulate all in memory
    while cursor:
        params["cursor"] = cursor
        page_raw = client.get("/markets", params=params)
        page = MarketsResponse.model_validate(page_raw)
        all_markets.extend(page.markets)
        pull_pages += 1
        cursor = page.cursor

        if pull_pages % 10 == 0:
            pull_logger.info("Page %d: %d markets so far", pull_pages, len(all_markets))

    client.close()
    pull_total_markets = len(all_markets)

    # Write single parquet file to S3
    writer_fs = s3fs.S3FileSystem(
        key=SETTINGS.aws_access_key_id,
        secret=SETTINGS.aws_secret_access_key,
        client_kwargs={"region_name": SETTINGS.aws_region},
    )
    table = _markets_to_table(all_markets)
    with writer_fs.open(pull_s3_path, "wb") as f_out:
        pq.write_table(table, f_out)

    pull_elapsed = time.time() - pull_start

    pull_logger.info(
        "Done: %d markets, %d pages, %.1fs, filter=%s",
        pull_total_markets, pull_pages, pull_elapsed, pull_filter_used,
    )

    return pull_elapsed, pull_filter_used, pull_pages, pull_total_markets


@app.cell
def results_summary(
    SETTINGS, mo, pd, pull_elapsed, pull_filter_used,
    pull_pages, pull_s3_path, pull_total_markets, s3fs,
):
    mo.md("## Results")

    # Get file size from S3
    size_fs = s3fs.S3FileSystem(
        key=SETTINGS.aws_access_key_id,
        secret=SETTINGS.aws_secret_access_key,
        client_kwargs={"region_name": SETTINGS.aws_region},
    )
    # s3fs paths without the s3:// prefix
    s3_key = pull_s3_path.replace("s3://", "")
    file_info = size_fs.info(s3_key)
    file_size_mb = file_info["size"] / (1024 * 1024)

    markets_per_sec = pull_total_markets / pull_elapsed if pull_elapsed > 0 else 0

    summary_df = pd.DataFrame([
        {"Metric": "Total markets", "Value": f"{pull_total_markets:,}"},
        {"Metric": "Elapsed time", "Value": f"{pull_elapsed:.1f}s"},
        {"Metric": "Markets / sec", "Value": f"{markets_per_sec:,.0f}"},
        {"Metric": "API pages", "Value": f"{pull_pages:,}"},
        {"Metric": "Filter used", "Value": pull_filter_used},
        {"Metric": "S3 path", "Value": pull_s3_path},
        {"Metric": "File size", "Value": f"{file_size_mb:.2f} MB"},
    ])

    mo.ui.table(summary_df, label="Daily Pull Summary")

    return ()


if __name__ == "__main__":
    app.run()
