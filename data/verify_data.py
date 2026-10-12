"""Verify the food-delivery CSVs in ``data/``.

Counts rows by streaming each CSV (the largest is ~1.3 GB, so it must not be
loaded into memory) and, when the repo-root ``.env`` has Postgres credentials,
compares the counts against the loaded Postgres tables.

Usage:
    python data/verify_data.py            # CSV row counts only
    python data/verify_data.py --db       # also compare against the local Postgres
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

TABLES = [
    ("users.csv", "users"),
    ("restaurant.csv", "restaurant"),
    ("food.csv", "food"),
    ("menu.csv", "menu"),
    ("orders.csv", "orders"),
    ("order_items.csv", "order_items"),
    ("reviews.csv", "reviews"),
]


def detect_encoding(path: Path) -> str:
    try:
        path.open("rb").read(4096).decode("utf-8-sig")
        return "utf-8-sig"
    except UnicodeDecodeError:
        return "latin-1"


def count_csv(path: Path) -> tuple[int, int]:
    """Return (row_count, column_count) without loading the file into memory."""
    with path.open(encoding=detect_encoding(path), errors="replace", newline="") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            return 0, 0
        n = sum(1 for _ in reader)
    return n, len(header)


def db_counts() -> dict[str, int]:
    import psycopg2

    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "postgres"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        sslmode=os.environ.get("POSTGRES_SSLMODE", "prefer"),
    )
    counts: dict[str, int] = {}
    try:
        with conn.cursor() as cur:
            for _, table in TABLES:
                cur.execute(f'SELECT count(*) FROM public."{table}"')
                counts[table] = cur.fetchone()[0]
    finally:
        conn.close()
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", action="store_true", help="compare against the local Postgres table counts")
    args = parser.parse_args()

    counts: dict[str, int] = {}
    mismatches = 0
    for csv_name, table in TABLES:
        path = DATA_DIR / csv_name
        if not path.exists():
            print(f"{table:<12} MISSING {csv_name}")
            mismatches += 1
            continue
        rows, cols = count_csv(path)
        counts[table] = rows
        size_mb = path.stat().st_size / 1_048_576
        print(f"{table:<12} {rows:>12,} rows  {cols:>3} cols  {size_mb:>8.1f} MB")

    if args.db:
        load_dotenv(ROOT / ".env")
        try:
            db = db_counts()
        except Exception as exc:  # noqa: BLE001
            print(f"\nerror: could not read Postgres counts: {exc}", file=sys.stderr)
            return 1
        print("\nCSV vs DB:")
        for _, table in TABLES:
            csv_rows = counts.get(table)
            db_rows = db.get(table)
            flag = "OK" if csv_rows == db_rows else "MISMATCH"
            if flag != "OK":
                mismatches += 1
            print(f"  {table:<12} csv={csv_rows:>12,}  db={db_rows:>12,}  {flag}")

    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
