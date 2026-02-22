"""DuckDB connection pre-configured with S3 credentials for querying parquet on S3."""

from __future__ import annotations

import duckdb

from longshot.config import SETTINGS

# S3 paths for use in queries
MARKETS_ALL = f"s3://{SETTINGS.s3_bucket}/{SETTINGS.s3_prefix}/markets/all/*.parquet"
EVENTS_ALL = f"s3://{SETTINGS.s3_bucket}/{SETTINGS.s3_prefix}/events/data.parquet"
MARKETS_SNAPSHOT = (
    f"s3://{SETTINGS.s3_bucket}/{SETTINGS.s3_prefix}/markets/snapshot_date={{date}}/data.parquet"
)
TRADES_SNAPSHOT = (
    f"s3://{SETTINGS.s3_bucket}/{SETTINGS.s3_prefix}/trades/snapshot_date={{date}}/data.parquet"
)


def connect() -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection with S3 credentials installed."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_region = '{SETTINGS.aws_region}';
        SET s3_access_key_id = '{SETTINGS.aws_access_key_id}';
        SET s3_secret_access_key = '{SETTINGS.aws_secret_access_key}';
    """)
    return con
