"""Load the food-delivery CSV files in ``data/`` into the local Postgres.

Connection comes from the repo-root ``.env`` (POSTGRES_HOST/PORT/DB/USER/PASSWORD),
loaded with python-dotenv. Loading uses Postgres ``COPY`` (``FORMAT csv``) because
the raw CSVs are large — ~34M rows across the set, with ``orders.csv`` at 1.3 GB —
which HTTP/PostgREST batching cannot handle.

The target is the Dockerized Postgres in ``docker/postgres`` (start it from the
repo root with ``stack start postgres``). It has enough local disk for every
table, including ``orders`` and ``order_items``.

Each CSV has an unnamed leading index column (users/restaurant/food/menu); it is
dropped. Column names are mapped to snake_case DB columns (e.g. "Marital Status"
-> ``marital_status``). Empty fields become NULL; ``restaurant.rating`` '--'
becomes NULL.

Usage (from anywhere, run with the repo venv):
    python data/import_data.py --create                 # create tables + load all
    python data/import_data.py --only users --create
    python data/import_data.py --truncate               # reload cleanly (the loader APPENDS; --truncate avoids duplicates)
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import re
import sys
from pathlib import Path
from typing import Callable, Iterator

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data/dataset"

_NUMBER = re.compile(r"-?\d+(?:\.\d+)?")


def first_number(value: str) -> str | None:
    """Extract the leading number from a price string, else NULL.

    menu.price contains dirty values such as '₹200 FOR TWO'; this recovers 200.
    """
    match = _NUMBER.search(value.replace(",", ""))
    return match.group(0) if match else None

# (csv file, table, [(csv header, db column)], {db column: transform})
# Load order is FK-safe (parents first) even though no FKs are declared.
TABLES: list[tuple[str, str, list[tuple[str, str]], dict[str, Callable[[str], object]]]] = [
    ("users.csv", "users", [
        ("user_id", "user_id"),
        ("name", "name"),
        ("email", "email"),
        ("password", "password"),
        ("Age", "age"),
        ("Gender", "gender"),
        ("Marital Status", "marital_status"),
        ("Occupation", "occupation"),
        ("Monthly Income", "monthly_income"),
        ("Educational Qualifications", "educational_qualifications"),
        ("Family size", "family_size"),
    ], {}),
    ("restaurant.csv", "restaurant", [
        ("id", "id"),
        ("name", "name"),
        ("city", "city"),
        ("rating", "rating"),
        ("rating_count", "rating_count"),
        ("cost", "cost"),
        ("cuisine", "cuisine"),
        ("lic_no", "lic_no"),
        ("link", "link"),
        ("address", "address"),
        ("menu", "menu"),
    ], {"rating": lambda v: None if v == "--" else v}),
    ("food.csv", "food", [
        ("f_id", "f_id"),
        ("item", "item"),
        ("veg_or_non_veg", "veg_or_non_veg"),
    ], {}),
    ("menu.csv", "menu", [
        ("menu_id", "menu_id"),
        ("r_id", "r_id"),
        ("f_id", "f_id"),
        ("cuisine", "cuisine"),
        ("price", "price"),
    ], {"price": first_number}),
    ("orders.csv", "orders", [
        ("order_id", "order_id"),
        ("order_timestamp", "order_timestamp"),
        ("order_date", "order_date"),
        ("user_id", "user_id"),
        ("r_id", "r_id"),
        ("restaurant_city", "restaurant_city"),
        ("cuisine", "cuisine"),
        ("items_count", "items_count"),
        ("sales_qty", "sales_qty"),
        ("subtotal", "subtotal"),
        ("discount", "discount"),
        ("delivery_fee", "delivery_fee"),
        ("gst", "gst"),
        ("sales_amount", "sales_amount"),
        ("currency", "currency"),
        ("payment_method", "payment_method"),
        ("order_status", "order_status"),
        ("customer_rating", "customer_rating"),
        ("delivery_time_min", "delivery_time_min"),
    ], {}),
    ("order_items.csv", "order_items", [
        ("order_item_id", "order_item_id"),
        ("order_id", "order_id"),
        ("r_id", "r_id"),
        ("f_id", "f_id"),
        ("price", "price"),
        ("quantity", "quantity"),
        ("line_amount", "line_amount"),
    ], {}),
    ("reviews.csv", "reviews", [
        ("review_id", "review_id"),
        ("order_id", "order_id"),
        ("user_id", "user_id"),
        ("restaurant_id", "restaurant_id"),
        ("rating", "rating"),
        ("comment", "comment"),
        ("review_date", "review_date"),
    ], {}),
]


def detect_encoding(data: bytes) -> str:
    try:
        data.decode("utf-8-sig")
        return "utf-8-sig"
    except UnicodeDecodeError:
        return "latin-1"


def _copy_rows(path: Path, mapping: list[tuple[str, str]], transforms: dict) -> Iterator[str]:
    """Yield CSV-formatted records (db columns only), streaming row by row."""
    encoding = detect_encoding(path.read_bytes()[:4096])
    with path.open(encoding=encoding, newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        idx = {}
        for csv_name, _ in mapping:
            try:
                idx[csv_name] = header.index(csv_name)
            except ValueError as exc:
                raise SystemExit(f"{path.name}: missing column {csv_name!r} (got {header})") from exc

        out = io.StringIO()
        writer = csv.writer(out, lineterminator="\n")
        for row in reader:
            record = []
            for csv_name, db_col in mapping:
                value = row[idx[csv_name]] if idx[csv_name] < len(row) else ""
                if value == "":
                    value = None
                elif db_col in transforms:
                    value = transforms[db_col](value)
                record.append(value)
            writer.writerow(record)
            data = out.getvalue()
            out.seek(0)
            out.truncate(0)
            yield data


def load_table(conn, csv_name: str, table: str, mapping, transforms, chunk_size: int, limit: int | None = None) -> int:
    path = DATA_DIR / csv_name
    if not path.exists():
        print(f"  skip {table}: {csv_name} not found")
        return 0
    cols = ", ".join(f'"{db}"' for _, db in mapping)
    sql = f'COPY public."{table}" ({cols}) FROM STDIN WITH (FORMAT csv)'

    # Feed rows in chunks (one COPY statement each) so a single huge statement
    # can't be cancelled by a statement_timeout; also gives progress output.
    loaded = 0
    buffer: list[str] = []

    def flush(records: list[str]) -> None:
        with conn.cursor() as cur:
            cur.copy_expert(sql, io.StringIO("".join(records)))

    for record in _copy_rows(path, mapping, transforms):
        if limit is not None and loaded + len(buffer) >= limit:
            break
        buffer.append(record)
        if len(buffer) >= chunk_size:
            flush(buffer)
            loaded += len(buffer)
            buffer = []
            print(f"  {table}: {loaded:,}", flush=True)
    if buffer:
        flush(buffer)
        loaded += len(buffer)

    with conn.cursor() as cur:
        cur.execute(f'SELECT count(*) FROM public."{table}"')
        total = cur.fetchone()[0]
    print(f"  {table}: loaded, now {total:,} rows", flush=True)
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description="Load data/*.csv into the local pipeline Postgres")
    parser.add_argument("--only", action="append", metavar="TABLE", help="load only this table (repeatable)")
    parser.add_argument("--create", action="store_true", help="apply data/ddl.sql before loading")
    parser.add_argument("--truncate", action="store_true", help="truncate each table before loading")
    parser.add_argument("--copy-chunk", type=int, default=250_000, help="rows per COPY statement")
    parser.add_argument("--limit", type=int, default=None, help="max rows per table (for quick test loads)")
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")

    params = dict(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "postgres"),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        sslmode=os.environ.get("POSTGRES_SSLMODE", "prefer"),
    )
    if not params["user"] or not params["password"]:
        print("error: POSTGRES_USER/PASSWORD must be set in the repo-root .env", file=sys.stderr)
        return 1

    conn = psycopg2.connect(**params)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")
    except psycopg2.Error:
        pass
    try:
        if args.create:
            ddl = (DATA_DIR / "ddl.sql").read_text(encoding="utf-8")
            with conn.cursor() as cur:
                cur.execute(ddl)
            print("applied ddl.sql")

        selected = set(args.only) if args.only else None
        for csv_name, table, mapping, transforms in TABLES:
            if selected is not None and table not in selected:
                continue
            if args.truncate:
                with conn.cursor() as cur:
                    cur.execute(f'TRUNCATE public."{table}"')
                print(f"  {table}: truncated")
            print(f"loading {table} from {csv_name} ...", flush=True)
            load_table(conn, csv_name, table, mapping, transforms, args.copy_chunk, args.limit)
    finally:
        conn.close()
    print("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
