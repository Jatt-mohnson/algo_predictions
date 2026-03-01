"""DuckDB query helper for the ud-picks S3 parquet archive.

Usage:
    uv run ud-query                         # show archive summary
    uv run ud-query --table picks           # recent picks (last 7 days)
    uv run ud-query --table draftkings      # recent DraftKings odds
    uv run ud-query --days 14 --table picks # extend the lookback window
    uv run ud-query --interactive           # drop into a Python REPL with con ready
"""

import argparse
import code
import os

import duckdb
import pandas as pd

DEFAULT_PREFIX = "ud-picks"
TABLES = ("underdog", "kalshi", "draftkings", "pinnacle", "picks")


def connect(bucket: str | None = None, region: str = "us-east-1") -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection configured to read parquet files from S3.

    Credentials are sourced from boto3 (env vars, ~/.aws/credentials, IAM role, etc.)
    so whatever AWS auth method you normally use will work here.
    """
    import boto3

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")

    session = boto3.Session()
    creds = session.get_credentials()
    region = session.region_name or region

    if creds:
        frozen = creds.get_frozen_credentials()
        token_line = f"SESSION_TOKEN '{frozen.token}'," if frozen.token else ""
        con.execute(f"""
            CREATE SECRET (
                TYPE S3,
                KEY_ID '{frozen.access_key}',
                SECRET '{frozen.secret_key}',
                {token_line}
                REGION '{region}'
            )
        """)

    return con


def table_path(table: str, bucket: str, prefix: str = DEFAULT_PREFIX) -> str:
    """Return the S3 glob path for a table's parquet files."""
    return f"s3://{bucket}/{prefix}/{table}/**/*.parquet"


def summary(con: duckdb.DuckDBPyConnection, bucket: str, prefix: str = DEFAULT_PREFIX) -> None:
    """Print row counts per table for the last 7 days."""
    print(f"\nArchive summary — s3://{bucket}/{prefix}/\n")
    any_data = False
    for table in TABLES:
        path = table_path(table, bucket, prefix)
        try:
            result = con.execute(f"""
                SELECT dt, COUNT(*) AS rows, MAX(snapshot_time) AS latest_run
                FROM read_parquet('{path}', hive_partitioning=true)
                GROUP BY dt
                ORDER BY dt DESC
                LIMIT 7
            """).df()
        except Exception:
            continue
        if result.empty:
            continue
        any_data = True
        print(f"  {table}:")
        for _, row in result.iterrows():
            print(f"    {row['dt']}  {int(row['rows'])} rows  (latest: {row['latest_run']})")
    if not any_data:
        print("  No data found. Run `uv run ud-picks --archive` to start archiving.")
    print()


def query_table(
    con: duckdb.DuckDBPyConnection,
    table: str,
    bucket: str,
    days: int = 7,
    prefix: str = DEFAULT_PREFIX,
) -> pd.DataFrame:
    """Return the last N days of rows from a table as a DataFrame."""
    from datetime import date, timedelta
    since = (date.today() - timedelta(days=days)).isoformat()
    path = table_path(table, bucket, prefix)
    return con.execute(f"""
        SELECT *
        FROM read_parquet('{path}', hive_partitioning=true)
        WHERE dt >= '{since}'
        ORDER BY snapshot_time DESC
    """).df()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Query the ud-picks S3 parquet archive with DuckDB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run ud-query
  uv run ud-query --table picks --days 14
  uv run ud-query --interactive
        """,
    )
    parser.add_argument("--bucket", default=None,
                        help="S3 bucket (default: S3_BUCKET env var)")
    parser.add_argument("--table", choices=list(TABLES),
                        help="Show recent rows from a specific table")
    parser.add_argument("--days", type=int, default=7,
                        help="Lookback window in days (default: 7)")
    parser.add_argument("--interactive", "-i", action="store_true",
                        help="Open a Python REPL with a configured DuckDB connection")
    args = parser.parse_args()

    bucket = args.bucket or os.getenv("S3_BUCKET")
    if not bucket:
        print("S3_BUCKET not set. Pass --bucket or add S3_BUCKET to .env")
        return

    con = connect()

    if args.table:
        df = query_table(con, args.table, bucket, days=args.days)
        if df.empty:
            print(f"No {args.table} data in the last {args.days} days.")
        else:
            print(df.to_string(index=False))
        return

    summary(con, bucket)

    if args.interactive:
        banner = (
            f"DuckDB — s3://{bucket}/{DEFAULT_PREFIX}/\n\n"
            f"  con     → configured DuckDB connection\n"
            f"  bucket  → '{bucket}'\n\n"
            f"Quick start:\n"
            f"  from src.query import query_table, table_path\n"
            f"  query_table(con, 'picks', bucket, days=14)\n"
            f"  con.sql(\"SELECT * FROM read_parquet(table_path('picks', bucket), "
            f"hive_partitioning=true) WHERE edge > 5\").df()\n"
        )
        code.interact(banner=banner, local={
            "con": con, "bucket": bucket,
            "pd": pd, "duckdb": duckdb,
            "table_path": table_path, "query_table": query_table,
        })
    else:
        print("Options: --table <name>  |  --interactive")


if __name__ == "__main__":
    main()
