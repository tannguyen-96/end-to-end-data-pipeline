import os
import json
from datetime import datetime, timezone

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
import snowflake.connector as conn
from pgvector.psycopg2 import register_vector

load_dotenv()

# Configuration
EMBED_DIM = int(os.getenv("EMBED_DIM", "768"))
BATCH = 1000
CHUNK_SIZE = 200
# Runaway guard only; the loop is expected to end on an empty page.
MAX_PAGES = int(os.getenv("EMBED_COPY_MAX_PAGES", "200"))
# BATCH = 10
# CHUNK_SIZE = 5

SNOWFLAKE_SOURCE_TABLE = "ZOMATO.AI.REVIEW_EMBEDDINGS"
POSTGRES_TARGET_TABLE = "public.review_embeddings"


def get_snowflake_connection():
    return conn.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "ZOMATO_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE", "ZOMATO"),
    )


def get_postgres_connection():
    conn_pg = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        sslmode=os.getenv("POSTGRES_SSLMODE", "prefer"),
    )
    register_vector(conn_pg)
    return conn_pg


def get_pg_cursor(cursor_pg):
    """Return the last (embedded_at, review_id) already committed to Postgres.

    A single-timestamp watermark cannot express progress through a group of rows
    that share one EMBEDDED_AT, so the cursor is the pair. Defaults to the epoch
    and review_id -1 when the sink is empty.
    """
    cursor_pg.execute(
        f"""
        SELECT embedded_at, review_id
        FROM {POSTGRES_TARGET_TABLE}
        ORDER BY embedded_at DESC, review_id DESC
        LIMIT 1
        """
    )
    row = cursor_pg.fetchone()
    if not row or row[0] is None:
        return datetime(1970, 1, 1, tzinfo=timezone.utc), -1

    embedded_at, review_id = row
    if embedded_at.tzinfo is None:
        embedded_at = embedded_at.replace(tzinfo=timezone.utc)
    return embedded_at, int(review_id)


def fetch_deltas_from_snowflake(cursor_sf, cursor_at: datetime, cursor_id: int, limit: int):
    """Fetch the next page strictly after (cursor_at, cursor_id).

    The pair predicate is what makes a page impossible to repeat: rows tied on
    EMBEDDED_AT are walked in review_id order instead of being re-returned.
    """
    cursor_sf.execute(
        f"""
        SELECT REVIEW_ID, ORDER_ID, RESTAURANT_ID, RATING, COMMENT,
               ROW_HASH, EMBEDDING, EMBEDDING_MODEL, EMBEDDED_AT
        FROM {SNOWFLAKE_SOURCE_TABLE}
        WHERE (EMBEDDED_AT > %s)
           OR (EMBEDDED_AT = %s AND REVIEW_ID > %s)
        ORDER BY EMBEDDED_AT ASC, REVIEW_ID ASC
        LIMIT {limit}
        """,
        (cursor_at, cursor_at, cursor_id),
    )
    return cursor_sf.fetchall()


def _to_float_list(embedding):
    if embedding is None:
        return None
    if isinstance(embedding, list):
        return [float(x) for x in embedding]
    if isinstance(embedding, str):
        try:
            parsed = json.loads(embedding)
            if isinstance(parsed, list):
                return [float(x) for x in parsed]
        except Exception:
            pass
    return list(embedding)


def save_to_postgres(cursor_pg, rows):
    if not rows:
        return 0
    upsert_sql = f"""
        INSERT INTO {POSTGRES_TARGET_TABLE}
            (review_id, order_id, restaurant_id, rating, comment, row_hash,
             embedding, embedding_model, embedded_at)
        VALUES %s
        ON CONFLICT (review_id) DO UPDATE
        SET
            order_id = EXCLUDED.order_id,
            restaurant_id = EXCLUDED.restaurant_id,
            rating = EXCLUDED.rating,
            comment = EXCLUDED.comment,
            row_hash = EXCLUDED.row_hash,
            embedding = EXCLUDED.embedding,
            embedding_model = EXCLUDED.embedding_model,
            embedded_at = EXCLUDED.embedded_at
        WHERE {POSTGRES_TARGET_TABLE}.row_hash IS DISTINCT FROM EXCLUDED.row_hash
    """
    template = f"(%s, %s, %s, %s, %s, %s, %s::vector({EMBED_DIM}), %s, %s)"

    buffer = []
    affected = 0
    for r in rows:
        review_id, order_id, restaurant_id, rating, comment, row_hash, embedding, embedding_model, embedded_at = r
        embedding_list = _to_float_list(embedding)
        if embedding_list is not None and len(embedding_list) != EMBED_DIM:
            try:
                print(f"Warning: embedding dim mismatch for review_id={review_id}: {len(embedding_list)} != {EMBED_DIM}")
            except Exception:
                pass
        buffer.append(
            (
                review_id,
                order_id,
                restaurant_id,
                rating,
                comment,
                row_hash,
                embedding_list,
                embedding_model,
                embedded_at,
            )
        )
        if len(buffer) >= CHUNK_SIZE:
            # One statement per chunk instead of one execution per row. Note this
            # does NOT batch the HNSW index work: pgvector maintains the graph per
            # row via the index AM, so only round trips and parse/plan are saved.
            execute_values(cursor_pg, upsert_sql, buffer, template=template, page_size=CHUNK_SIZE)
            affected += cursor_pg.rowcount  # real upserts; hash-match rows are skipped
            buffer.clear()
    if buffer:
        execute_values(cursor_pg, upsert_sql, buffer, template=template, page_size=CHUNK_SIZE)
        affected += cursor_pg.rowcount
        buffer.clear()
    return affected


def process_deltas(sf_rows, cursor_pg):
    if not sf_rows:
        return 0

    total_saved = 0
    try:
        total_saved = save_to_postgres(cursor_pg, sf_rows)
        return total_saved
    except Exception:
        raise


def main():
    conn_sf = None
    cur_sf = None
    conn_pg_check = None
    cur_pg_check = None
    try:
        conn_sf = get_snowflake_connection()
        cur_sf = conn_sf.cursor()

        conn_pg_check = get_postgres_connection()
        cur_pg_check = conn_pg_check.cursor()
        
        cursor_at, cursor_id = get_pg_cursor(cur_pg_check)
        print(f"Starting cursor: ({cursor_at}, {cursor_id})")

        pages = 0
        total_affected = 0

        # Each page ends with a strictly greater cursor, so this terminates once
        # Snowflake has no row after it. MAX_PAGES is a runaway guard, not the
        # expected exit.
        while pages < MAX_PAGES:
            deltas = fetch_deltas_from_snowflake(cur_sf, cursor_at, cursor_id, BATCH)
            if not deltas:
                print(f"Drained: no rows after ({cursor_at}, {cursor_id}).")
                break

            print(f"Page {pages + 1}: {len(deltas)} row(s) after ({cursor_at}, {cursor_id})")
            affected = process_deltas(deltas, cur_pg_check)

            # Commit before advancing, so a crash re-reads from the last page that
            # actually landed. Replays are no-ops thanks to the row_hash guard.
            conn_pg_check.commit()

            last = deltas[-1]
            cursor_at, cursor_id = last[8], int(last[0])
            total_affected += affected
            pages += 1
            print(f"  -> upserted {affected}, cursor now ({cursor_at}, {cursor_id})")

        if pages >= MAX_PAGES:
            print(f"Stopped at MAX_PAGES={MAX_PAGES}; more rows may remain.")
        print(f"Done: {total_affected} row(s) upserted over {pages} page(s) into {POSTGRES_TARGET_TABLE}")
    except Exception as e:
        print(f"An error occurred while copying embeddings to Postgres: {e}")
        raise
    finally:
        if cur_sf:
            try:
                cur_sf.close()
            except Exception:
                pass
        if conn_sf:
            try:
                conn_sf.close()
            except Exception:
                pass
        if cur_pg_check:
            try:
                cur_pg_check.close()
            except Exception:
                pass
        if conn_pg_check:
            try:
                conn_pg_check.close()
            except Exception:
                pass

if __name__ == "__main__":
    main()