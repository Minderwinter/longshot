# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "marimo",
#     "boto3",
#     "pandas",
#     "pyarrow",
#     "s3fs",
#     "python-dotenv",
# ]
# ///

import marimo

__generated_with = "0.10.0"
app = marimo.App(width="medium")


@app.cell
def setup_imports():
    import marimo as mo
    import pyarrow as pa
    import pyarrow.parquet as pq
    import s3fs
    import pandas as pd
    import logging

    from longshot.storage.athena import query as athena_query
    from longshot.storage.s3 import MARKETS_SCHEMA, TRADES_SCHEMA
    from longshot.config import SETTINGS

    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)

    return MARKETS_SCHEMA, SETTINGS, TRADES_SCHEMA, athena_query, log, mo, pa, pd, pq, s3fs


@app.cell
def snapshot_params(mo):
    from datetime import datetime, timezone

    SNAPSHOT_DATE = "2025-01-01"
    SNAPSHOT_HOUR = 20

    snapshot_dt = datetime(2025, 1, 1, SNAPSHOT_HOUR, 0, 0, tzinfo=timezone.utc)
    SNAPSHOT_ISO = snapshot_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    SNAPSHOT_UNIX = int(snapshot_dt.timestamp())
    SNAPSHOT_MIN_TS = SNAPSHOT_UNIX - 86400  # 24 hours before snapshot

    mo.md(
        f"""
        # Historical Snapshot Pipeline

        Build a point-in-time market snapshot from existing Athena data,
        fetch trades from the Kalshi API (last 24h), and register both as Athena tables.

        | Parameter | Value |
        |-----------|-------|
        | Snapshot date | `{SNAPSHOT_DATE}` |
        | Snapshot hour (UTC) | `{SNAPSHOT_HOUR}` |
        | ISO timestamp | `{SNAPSHOT_ISO}` |
        | Unix timestamp | `{SNAPSHOT_UNIX}` |
        | Trades window | 24h (min_ts={SNAPSHOT_MIN_TS}) |
        """
    )

    return SNAPSHOT_DATE, SNAPSHOT_HOUR, SNAPSHOT_ISO, SNAPSHOT_MIN_TS, SNAPSHOT_UNIX


@app.cell
def s3_paths(SETTINGS, SNAPSHOT_DATE, SNAPSHOT_HOUR, mo):
    AD_HOC_PREFIX = f"s3://{SETTINGS.s3_bucket}/{SETTINGS.s3_prefix}/ad-hoc"

    MARKETS_S3_DIR = (
        f"{AD_HOC_PREFIX}/market_snapshots/"
        f"snapshot_date={SNAPSHOT_DATE}/snapshot_hour={SNAPSHOT_HOUR}"
    )
    MARKETS_S3_PATH = f"{MARKETS_S3_DIR}/data.parquet"

    TRADES_S3_DIR = (
        f"{AD_HOC_PREFIX}/trades/"
        f"snapshot_date={SNAPSHOT_DATE}/snapshot_hour={SNAPSHOT_HOUR}"
    )
    TRADES_S3_PATH = f"{TRADES_S3_DIR}/data.parquet"

    mo.md(
        f"""
        ## S3 Paths

        - Markets: `{MARKETS_S3_PATH}`
        - Trades: `{TRADES_S3_PATH}`
        """
    )

    return AD_HOC_PREFIX, MARKETS_S3_PATH, TRADES_S3_PATH


@app.cell
def query_snapshot_markets(SNAPSHOT_ISO, athena_query, mo):
    mo.md("## Step 1: Query Snapshot Markets from Athena")

    snapshot_markets_df = athena_query(f"""
        SELECT
            m.ticker,
            m.event_ticker,
            m.title,
            m.status,
            m.yes_bid,
            m.yes_ask,
            m.no_bid,
            m.no_ask,
            m.last_price,
            m.volume,
            m.volume_24h,
            m.open_interest,
            m.close_time,
            m.open_time,
            m.result,
            m.created_time
        FROM markets m
        WHERE m.open_time <= '{SNAPSHOT_ISO}'
          AND m.close_time > '{SNAPSHOT_ISO}'
    """)

    mo.md(f"Found **{len(snapshot_markets_df):,}** markets open at `{SNAPSHOT_ISO}`")

    return (snapshot_markets_df,)


@app.cell
def write_markets_to_s3(
    MARKETS_S3_PATH, MARKETS_SCHEMA, SETTINGS, log, mo, pa, pq, s3fs, snapshot_markets_df
):
    mo.md("## Step 2: Write Markets to S3")

    markets_fs = s3fs.S3FileSystem(
        key=SETTINGS.aws_access_key_id,
        secret=SETTINGS.aws_secret_access_key,
        client_kwargs={"region_name": SETTINGS.aws_region},
    )

    markets_table = pa.Table.from_pandas(
        snapshot_markets_df[MARKETS_SCHEMA.names],
        schema=MARKETS_SCHEMA,
        preserve_index=False,
    )

    with markets_fs.open(MARKETS_S3_PATH, "wb") as f_markets:
        pq.write_table(markets_table, f_markets)

    markets_written_count = len(snapshot_markets_df)
    log.info("Wrote %d markets to %s", markets_written_count, MARKETS_S3_PATH)
    mo.md(f"Wrote **{markets_written_count:,}** markets to `{MARKETS_S3_PATH}`")

    return (markets_written_count,)


@app.cell
def fetch_snapshot_trades(SNAPSHOT_MIN_TS, SNAPSHOT_UNIX, log, mo, snapshot_markets_df):
    mo.md("## Step 3: Fetch Trades from Kalshi API (24h window)")

    from longshot.api.client import KalshiClient
    from longshot.api.rate_limiter import TokenBucket
    from longshot.ingestion.trades import fetch_all_trades

    snapshot_tickers = snapshot_markets_df["ticker"].tolist()
    log.info(
        "Fetching trades for %d tickers, ts range [%d, %d]",
        len(snapshot_tickers), SNAPSHOT_MIN_TS, SNAPSHOT_UNIX,
    )

    trade_limiter = TokenBucket(rate=10.0)
    with KalshiClient(limiter=trade_limiter) as trade_client:
        fetched_trades = fetch_all_trades(
            client=trade_client,
            limiter=trade_limiter,
            tickers=snapshot_tickers,
            max_ts=SNAPSHOT_UNIX,
            min_ts=SNAPSHOT_MIN_TS,
        )

    mo.md(f"Fetched **{len(fetched_trades):,}** trades across **{len(snapshot_tickers):,}** tickers (24h window)")

    return (fetched_trades,)


@app.cell
def write_trades_to_s3(
    SETTINGS, TRADES_S3_PATH, TRADES_SCHEMA, fetched_trades, log, mo, pa, pq, s3fs
):
    mo.md("## Step 4: Write Trades to S3")

    trades_fs = s3fs.S3FileSystem(
        key=SETTINGS.aws_access_key_id,
        secret=SETTINGS.aws_secret_access_key,
        client_kwargs={"region_name": SETTINGS.aws_region},
    )

    trades_arrays = {
        "trade_id": [t.trade_id for t in fetched_trades],
        "ticker": [t.ticker for t in fetched_trades],
        "yes_price": [t.yes_price for t in fetched_trades],
        "no_price": [t.no_price for t in fetched_trades],
        "count": [t.count for t in fetched_trades],
        "taker_side": [t.taker_side for t in fetched_trades],
        "created_time": [t.created_time for t in fetched_trades],
    }
    trades_table = pa.table(trades_arrays, schema=TRADES_SCHEMA)

    with trades_fs.open(TRADES_S3_PATH, "wb") as f_trades:
        pq.write_table(trades_table, f_trades)

    trades_written_count = len(fetched_trades)
    log.info("Wrote %d trades to %s", trades_written_count, TRADES_S3_PATH)
    mo.md(f"Wrote **{trades_written_count:,}** trades to `{TRADES_S3_PATH}`")

    return (trades_written_count,)


@app.cell
def register_athena_markets_table(AD_HOC_PREFIX, athena_query, mo):
    mo.md("## Step 5: Register Athena Tables")

    markets_location = f"{AD_HOC_PREFIX}/market_snapshots/"

    athena_query(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS longshot.ad_hoc_market_snapshots (
            ticker          STRING,
            event_ticker    STRING,
            title           STRING,
            status          STRING,
            yes_bid         DOUBLE,
            yes_ask         DOUBLE,
            no_bid          DOUBLE,
            no_ask          DOUBLE,
            last_price      DOUBLE,
            volume          BIGINT,
            volume_24h      BIGINT,
            open_interest   BIGINT,
            close_time      STRING,
            open_time       STRING,
            result          STRING,
            created_time    STRING
        )
        PARTITIONED BY (snapshot_date STRING, snapshot_hour INT)
        STORED AS PARQUET
        LOCATION '{markets_location}'
        TBLPROPERTIES (
            'projection.enabled' = 'true',
            'projection.snapshot_date.type' = 'date',
            'projection.snapshot_date.format' = 'yyyy-MM-dd',
            'projection.snapshot_date.range' = '2024-01-01,2026-12-31',
            'projection.snapshot_hour.type' = 'integer',
            'projection.snapshot_hour.range' = '0,23',
            'storage.location.template' = '{markets_location}snapshot_date=${{snapshot_date}}/snapshot_hour=${{snapshot_hour}}'
        )
    """)

    mo.md(f"Registered `longshot.ad_hoc_market_snapshots` at `{markets_location}`")
    return ()


@app.cell
def register_athena_trades_table(AD_HOC_PREFIX, athena_query, mo):
    mo.md("## Step 6: Register Trades Table")

    trades_location = f"{AD_HOC_PREFIX}/trades/"

    athena_query(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS longshot.ad_hoc_trades (
            trade_id        STRING,
            ticker          STRING,
            yes_price       DOUBLE,
            no_price        DOUBLE,
            count           BIGINT,
            taker_side      STRING,
            created_time    STRING
        )
        PARTITIONED BY (snapshot_date STRING, snapshot_hour INT)
        STORED AS PARQUET
        LOCATION '{trades_location}'
        TBLPROPERTIES (
            'projection.enabled' = 'true',
            'projection.snapshot_date.type' = 'date',
            'projection.snapshot_date.format' = 'yyyy-MM-dd',
            'projection.snapshot_date.range' = '2024-01-01,2026-12-31',
            'projection.snapshot_hour.type' = 'integer',
            'projection.snapshot_hour.range' = '0,23',
            'storage.location.template' = '{trades_location}snapshot_date=${{snapshot_date}}/snapshot_hour=${{snapshot_hour}}'
        )
    """)

    mo.md(f"Registered `longshot.ad_hoc_trades` at `{trades_location}`")
    return ()


@app.cell
def verify_snapshot(SNAPSHOT_DATE, SNAPSHOT_HOUR, athena_query, mo):
    mo.md("## Step 7: Verification")

    market_verify = athena_query(f"""
        SELECT
            count(*)         AS market_count,
            avg(yes_ask)     AS avg_yes_ask,
            avg(volume)      AS avg_volume,
            count(DISTINCT event_ticker) AS unique_events
        FROM longshot.ad_hoc_market_snapshots
        WHERE snapshot_date = '{SNAPSHOT_DATE}'
          AND snapshot_hour = {SNAPSHOT_HOUR}
    """)

    trade_verify = athena_query(f"""
        SELECT
            count(*)            AS trade_count,
            count(DISTINCT ticker) AS unique_tickers,
            avg(yes_price)      AS avg_yes_price,
            min(created_time)   AS earliest_trade,
            max(created_time)   AS latest_trade
        FROM longshot.ad_hoc_trades
        WHERE snapshot_date = '{SNAPSHOT_DATE}'
          AND snapshot_hour = {SNAPSHOT_HOUR}
    """)

    mv = market_verify.iloc[0]
    tv = trade_verify.iloc[0]

    mo.md(
        f"""
        ### Market Snapshot Verification

        | Metric | Value |
        |--------|-------|
        | Market count | {int(mv.market_count):,} |
        | Avg yes_ask | {mv.avg_yes_ask:.1f} |
        | Avg volume | {mv.avg_volume:.1f} |
        | Unique events | {int(mv.unique_events):,} |

        ### Trade Verification

        | Metric | Value |
        |--------|-------|
        | Trade count | {int(tv.trade_count):,} |
        | Unique tickers | {int(tv.unique_tickers):,} |
        | Avg yes_price | {tv.avg_yes_price:.1f} |
        | Earliest trade | {tv.earliest_trade} |
        | Latest trade | {tv.latest_trade} |
        """
    )
    return ()


@app.cell
def summary_output(
    MARKETS_S3_PATH, TRADES_S3_PATH, SNAPSHOT_DATE, SNAPSHOT_HOUR,
    markets_written_count, trades_written_count, mo, pd
):
    mo.md("## Summary")

    summary_data = pd.DataFrame([
        {"Item": "Snapshot Date", "Value": SNAPSHOT_DATE},
        {"Item": "Snapshot Hour (UTC)", "Value": str(SNAPSHOT_HOUR)},
        {"Item": "Markets Written", "Value": f"{markets_written_count:,}"},
        {"Item": "Trades Written", "Value": f"{trades_written_count:,}"},
        {"Item": "Markets S3 Path", "Value": MARKETS_S3_PATH},
        {"Item": "Trades S3 Path", "Value": TRADES_S3_PATH},
        {"Item": "Athena Markets Table", "Value": "longshot.ad_hoc_market_snapshots"},
        {"Item": "Athena Trades Table", "Value": "longshot.ad_hoc_trades"},
    ])

    mo.ui.table(summary_data, label="Pipeline Summary")
    return ()


if __name__ == "__main__":
    app.run()
