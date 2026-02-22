"""Athena query helper — runs SQL, polls for completion, returns a pandas DataFrame."""

from __future__ import annotations

import time

import boto3
import pandas as pd

from longshot.config import SETTINGS

DATABASE = "longshot"
OUTPUT_LOCATION = f"s3://{SETTINGS.s3_bucket}/athena-results/"


def _client():
    return boto3.client(
        "athena",
        region_name=SETTINGS.aws_region,
        aws_access_key_id=SETTINGS.aws_access_key_id,
        aws_secret_access_key=SETTINGS.aws_secret_access_key,
    )


def query(sql: str, *, database: str = DATABASE) -> pd.DataFrame:
    """Execute *sql* on Athena and return the result as a DataFrame."""
    client = _client()
    resp = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": OUTPUT_LOCATION},
    )
    qid = resp["QueryExecutionId"]

    # Poll until done
    while True:
        status = client.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.5)

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
        raise RuntimeError(f"Athena query {state}: {reason}")

    # Paginate results
    rows: list[list[str]] = []
    headers: list[str] = []
    paginator = client.get_paginator("get_query_results")
    for page in paginator.paginate(QueryExecutionId=qid):
        result_set = page["ResultSet"]
        if not headers:
            headers = [
                col["Name"] for col in result_set["ResultSetMetadata"]["ColumnInfo"]
            ]
            # First page includes header row — skip it
            data_rows = result_set["Rows"][1:]
        else:
            data_rows = result_set["Rows"]
        for row in data_rows:
            rows.append([col.get("VarCharValue") for col in row["Data"]])

    df = pd.DataFrame(rows, columns=headers)

    # Athena returns everything as strings — auto-cast numeric columns.
    for c in df.columns:
        try:
            df[c] = pd.to_numeric(df[c])
        except (ValueError, TypeError):
            pass

    return df
