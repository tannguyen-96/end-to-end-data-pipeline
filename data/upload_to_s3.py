"""Upload the large source CSVs to S3 (optional raw staging).

``orders`` and ``order_items`` are the two biggest files. This is only needed if you
want the raw CSVs staged in S3 directly; the pipeline otherwise exports tables from
the local Postgres to S3 via ``docker/airflow/dags/export_tables.py``.
Reads the repo-root ``.env`` (S3_BUCKET, AWS_DEFAULT_REGION; AWS creds from env or
``~/.aws``).

Usage:
    python data/upload_to_s3.py --dry-run          # show what would upload
    python data/upload_to_s3.py                    # upload orders + order_items
    python data/upload_to_s3.py --only orders
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import boto3
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

# The two largest tables (optional raw staging).
FILES = [
    ("orders.csv", "orders"),
    ("order_items.csv", "order_items"),
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", action="append", metavar="TABLE", help="upload only this table (repeatable)")
    parser.add_argument("--prefix", default="raw", help="S3 key prefix (default: raw)")
    parser.add_argument("--dry-run", action="store_true", help="print actions without uploading")
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")
    bucket = os.environ.get("S3_BUCKET")
    region = os.environ.get("AWS_DEFAULT_REGION")
    if not bucket:
        print("error: S3_BUCKET must be set in the repo-root .env", file=sys.stderr)
        return 1

    selected = set(args.only) if args.only else None
    s3 = boto3.client("s3", region_name=region) if region else boto3.client("s3")

    total_mb = 0.0
    for csv_name, table in FILES:
        if selected is not None and table not in selected:
            continue
        path = DATA_DIR / csv_name
        if not path.exists():
            print(f"  skip {table}: {csv_name} not found")
            continue
        key = f"{args.prefix}/{table}/{table}.csv"
        mb = path.stat().st_size / 1_048_576
        total_mb += mb
        if args.dry_run:
            print(f"  [dry-run] {csv_name} ({mb:.1f} MB) -> s3://{bucket}/{key}")
            continue
        s3.upload_file(str(path), bucket, key)
        print(f"  uploaded {csv_name} ({mb:.1f} MB) -> s3://{bucket}/{key}", flush=True)

    print(f"done ({total_mb:.1f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
