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
    import logging

    import httpx
    import pyarrow as pa
    import pyarrow.parquet as pq
    import s3fs
    import pandas as pd

    from longshot.api.client import KalshiClient
    from longshot.api.rate_limiter import TokenBucket
    from longshot.config import SETTINGS

    logging.basicConfig(level=logging.INFO)
    evt_logger = logging.getLogger("daily_event_pull")

    return (
        SETTINGS, TokenBucket, KalshiClient,
        evt_logger, httpx, mo, pa, pd, pq, s3fs,
    )


@app.cell
def event_pull_params(SETTINGS, mo):
    from datetime import datetime, timezone

    evt_now = datetime.now(timezone.utc)
    evt_date_str = evt_now.strftime("%Y-%m-%d")
    evt_hour = evt_now.hour
    evt_close_ts = int(evt_now.timestamp())
    evt_s3_path = (
        f"s3://{SETTINGS.s3_bucket}/{SETTINGS.s3_prefix}"
        f"/events/daily/date={evt_date_str}/hour={evt_hour:02d}/data.parquet"
    )

    mo.md(
        f"""
        # Daily Event Pull (with Nested Markets)

        Pull all events from Kalshi with `close_time` in the future,
        including nested markets as JSON.

        | Parameter | Value |
        |-----------|-------|
        | Timestamp (UTC) | `{evt_now.isoformat()}` |
        | Date | `{evt_date_str}` |
        | Hour | `{evt_hour:02d}` |
        | min_close_ts | `{evt_close_ts}` |
        | S3 path | `{evt_s3_path}` |
        """
    )

    return evt_close_ts, evt_date_str, evt_hour, evt_now, evt_s3_path


@app.cell
def pull_and_write_events(
    KalshiClient, SETTINGS, TokenBucket,
    evt_close_ts, evt_logger, evt_s3_path,
    httpx, mo, pa, pq, s3fs,
):
    import time
    import json

    mo.md("## Pulling events from Kalshi API...")

    limiter = TokenBucket(rate=10.0, burst=20.0)
    client = KalshiClient(limiter=limiter)

    # Try with both min_close_ts and with_nested_markets
    evt_filter_used = "min_close_ts + with_nested_markets"
    base_params: dict = {
        "limit": 200,
        "min_close_ts": evt_close_ts,
        "with_nested_markets": "true",
    }

    try:
        first_raw = client.get("/events", params=dict(base_params))
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 400:
            evt_logger.warning(
                "400 with min_close_ts + with_nested_markets — "
                "falling back to min_close_ts only"
            )
            evt_filter_used = "min_close_ts only"
            base_params = {"limit": 200, "min_close_ts": evt_close_ts}
            try:
                first_raw = client.get("/events", params=dict(base_params))
            except httpx.HTTPStatusError as exc2:
                if exc2.response.status_code == 400:
                    evt_logger.warning(
                        "400 with min_close_ts — falling back to no filters"
                    )
                    evt_filter_used = "no filters"
                    base_params = {"limit": 200}
                    first_raw = client.get("/events", params=dict(base_params))
                else:
                    raise
        else:
            raise

    # Accumulate raw event dicts
    all_events = list(first_raw.get("events", []))
    cursor = first_raw.get("cursor")
    evt_pages = 1

    evt_start = time.time()

    while cursor:
        page_params = dict(base_params)
        page_params["cursor"] = cursor
        page_raw = client.get("/events", params=page_params)
        all_events.extend(page_raw.get("events", []))
        evt_pages += 1
        cursor = page_raw.get("cursor")

        if evt_pages % 10 == 0:
            evt_logger.info(
                "Page %d: %d events so far", evt_pages, len(all_events)
            )

    client.close()
    evt_total = len(all_events)

    # Build table: 6 existing fields + markets_json
    evt_schema = pa.schema([
        pa.field("event_ticker", pa.string()),
        pa.field("category", pa.string()),
        pa.field("title", pa.string()),
        pa.field("sub_title", pa.string()),
        pa.field("mutually_exclusive", pa.bool_()),
        pa.field("series_ticker", pa.string()),
        pa.field("markets_json", pa.string()),
    ])

    table = pa.table(
        {
            "event_ticker": [e["event_ticker"] for e in all_events],
            "category": [e.get("category") for e in all_events],
            "title": [e.get("title") for e in all_events],
            "sub_title": [e.get("sub_title") for e in all_events],
            "mutually_exclusive": [e.get("mutually_exclusive") for e in all_events],
            "series_ticker": [e.get("series_ticker") for e in all_events],
            "markets_json": [
                json.dumps(e.get("markets", [])) for e in all_events
            ],
        },
        schema=evt_schema,
    )

    # Write single parquet to S3
    writer_fs = s3fs.S3FileSystem(
        key=SETTINGS.aws_access_key_id,
        secret=SETTINGS.aws_secret_access_key,
        client_kwargs={"region_name": SETTINGS.aws_region},
    )
    with writer_fs.open(evt_s3_path, "wb") as f_out:
        pq.write_table(table, f_out)

    evt_elapsed = time.time() - evt_start

    evt_logger.info(
        "Done: %d events, %d pages, %.1fs, filter=%s",
        evt_total, evt_pages, evt_elapsed, evt_filter_used,
    )

    return evt_elapsed, evt_filter_used, evt_pages, evt_total


@app.cell
def event_results_summary(
    SETTINGS, evt_elapsed, evt_filter_used, evt_pages,
    evt_s3_path, evt_total, mo, pd, pq, s3fs,
):
    mo.md("## Results")

    # Get file size from S3
    size_fs = s3fs.S3FileSystem(
        key=SETTINGS.aws_access_key_id,
        secret=SETTINGS.aws_secret_access_key,
        client_kwargs={"region_name": SETTINGS.aws_region},
    )
    s3_key = evt_s3_path.replace("s3://", "")
    file_info = size_fs.info(s3_key)
    evt_file_size_mb = file_info["size"] / (1024 * 1024)

    events_per_sec = evt_total / evt_elapsed if evt_elapsed > 0 else 0

    # Read back to check markets_json
    with size_fs.open(s3_key, "rb") as f_in:
        read_table = pq.read_table(f_in)
    markets_col = read_table.column("markets_json").to_pylist()
    has_nested = sum(1 for m in markets_col if m and m != "[]")

    summary_data = pd.DataFrame([
        {"Metric": "Total events", "Value": f"{evt_total:,}"},
        {"Metric": "Elapsed time", "Value": f"{evt_elapsed:.1f}s"},
        {"Metric": "Events / sec", "Value": f"{events_per_sec:,.0f}"},
        {"Metric": "API pages", "Value": f"{evt_pages:,}"},
        {"Metric": "Filter used", "Value": evt_filter_used},
        {"Metric": "S3 path", "Value": evt_s3_path},
        {"Metric": "File size", "Value": f"{evt_file_size_mb:.2f} MB"},
        {"Metric": "Events with markets", "Value": f"{has_nested:,}"},
    ])

    mo.ui.table(summary_data, label="Daily Event Pull Summary")

    # Category breakdown
    cat_col = read_table.column("category").to_pylist()
    cat_counts: dict[str, int] = {}
    for cat_val in cat_col:
        cat_key = cat_val or "Unknown"
        cat_counts[cat_key] = cat_counts.get(cat_key, 0) + 1
    cat_df = pd.DataFrame(
        [{"Category": k, "Count": v} for k, v in sorted(cat_counts.items(), key=lambda x: -x[1])]
    )
    mo.md("### Category Breakdown")
    mo.ui.table(cat_df, label="Categories")

    return ()


if __name__ == "__main__":
    app.run()
