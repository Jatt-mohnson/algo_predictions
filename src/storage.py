"""S3 archiving utilities for ud-picks run snapshots.

Writes Hive-partitioned parquet files:
    s3://{bucket}/{prefix}/{table}/dt={YYYY-MM-DD}/{HHMMSS}.parquet

Each file includes a snapshot_time column (ISO-8601 string) so runs within
the same date can be distinguished and filtered in DuckDB queries.
"""

import io
import os
from datetime import datetime

import pandas as pd

DEFAULT_PREFIX = "ud-picks"


def _to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    return buf.getvalue()


def upload_run_snapshot(
    source: str,
    picks: pd.DataFrame,
    bucket: str | None = None,
    prefix: str = DEFAULT_PREFIX,
) -> None:
    """Upload raw source DataFrames and picks to S3 as Hive-partitioned parquet.

    Each table is stored under its own prefix so DuckDB can query them
    independently with hive_partitioning=true:
        {prefix}/underdog/dt=YYYY-MM-DD/HHMMSS.parquet
        {prefix}/kalshi/dt=YYYY-MM-DD/HHMMSS.parquet
        {prefix}/picks/dt=YYYY-MM-DD/HHMMSS.parquet

    Args:
        source:  The --source flag value (kalshi, draftkings, pinnacle, both).
        picks:   The picks DataFrame from find_picks().
        bucket:  S3 bucket name. Falls back to S3_BUCKET env var.
        prefix:  Top-level key prefix (default: "ud-picks").
    """
    import boto3
    from dotenv import load_dotenv

    load_dotenv()

    from src.common import KALSHI_CSV, DRAFTKINGS_CSV, PINNACLE_CSV, UNDERDOG_CSV

    bucket = bucket or os.getenv("S3_BUCKET")
    if not bucket:
        print("S3_BUCKET not set — skipping archive.")
        return

    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H%M%S")
    snapshot_time = now.isoformat(timespec="seconds")

    source_files: list[tuple[str, str]] = [("underdog", UNDERDOG_CSV)]
    if source in ("kalshi", "both"):
        source_files.append(("kalshi", KALSHI_CSV))
    if source in ("draftkings", "both"):
        source_files.append(("draftkings", DRAFTKINGS_CSV))
    if source in ("pinnacle", "both"):
        source_files.append(("pinnacle", PINNACLE_CSV))

    s3 = boto3.client("s3")
    uploaded: list[str] = []

    for table, path in source_files:
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            continue
        try:
            df = pd.read_csv(path)
        except Exception as e:
            print(f"  Warning: could not read {path}: {e}")
            continue
        df["snapshot_time"] = snapshot_time
        key = f"{prefix}/{table}/dt={date_str}/{time_str}.parquet"
        s3.put_object(Bucket=bucket, Key=key, Body=_to_parquet_bytes(df),
                      ContentType="application/octet-stream")
        uploaded.append(table)

    if not picks.empty:
        df = picks.copy()
        df["snapshot_time"] = snapshot_time
        key = f"{prefix}/picks/dt={date_str}/{time_str}.parquet"
        s3.put_object(Bucket=bucket, Key=key, Body=_to_parquet_bytes(df),
                      ContentType="application/octet-stream")
        uploaded.append("picks")

    if uploaded:
        print(f"Archived to s3://{bucket}/{prefix}/ dt={date_str} ({', '.join(uploaded)})")
    else:
        print("Nothing to archive — no source files found.")
