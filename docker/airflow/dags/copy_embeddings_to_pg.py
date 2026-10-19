import os
import json
from datetime import datetime, timezone

from dotenv import load_dotenv
import psycopg2
import snowflake.connector as conn
from pgvector.psycopg2 import register_vector

load_dotenv()

# Configuration
EMBED_DIM = int(os.getenv("EMBED_DIM", "768"))
# BATCH = 1000
# CHUNK_SIZE = 500
BATCH = 10
CHUNK_SIZE = 5

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


def get_pg_watermark(cursor_pg) -> datetime:
    cursor_pg.execute(
        f"SELECT COALESCE(MAX(embedded_at), '1970-01-01 00:00:00+00'::timestamptz) FROM {POSTGRES_TARGET_TABLE} LIMIT 1"
    )
    result = cursor_pg.fetchone()
    watermark = result[0] if result and result[0] is not None else datetime(1970, 1, 1, tzinfo=timezone.utc)
    if watermark.tzinfo is None:
        watermark = watermark.replace(tzinfo=timezone.utc)
    return watermark


def fetch_deltas_from_snowflake(cursor_sf, watermark: datetime, limit: int):
    cursor_sf.execute(
        f"""
        SELECT REVIEW_ID, ORDER_ID, RESTAURANT_ID, RATING, COMMENT,
               ROW_HASH, EMBEDDING, EMBEDDING_MODEL, EMBEDDED_AT
        FROM {SNOWFLAKE_SOURCE_TABLE}
        WHERE EMBEDDED_AT >= %s
        ORDER BY EMBEDDED_AT ASC, REVIEW_ID ASC
        LIMIT {limit}
        """,
        (watermark,),
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
        VALUES (%s, %s, %s, %s, %s, %s, %s::vector({EMBED_DIM}), %s, %s)
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

    buffer = []
    saved = 0
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
            cursor_pg.executemany(upsert_sql, buffer)
            saved += len(buffer)
            buffer.clear()
    if buffer:
        cursor_pg.executemany(upsert_sql, buffer)
        saved += len(buffer)
        buffer.clear()
    return saved


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
        
        count = 0
        while count < 1:
            watermark = get_pg_watermark(cur_pg_check)
            print(f"Current watermark: {watermark}")
            
            deltas = fetch_deltas_from_snowflake(cur_sf, watermark, BATCH)
            print(f"Fetched {len(deltas)} delta(s) from Snowflake.")
            if not deltas:
                print("No new embeddings to copy to Postgres.")
                return
            
            print(f"Copying {len(deltas)} embedding(s) from Snowflake to Postgres (watermark={watermark})...")
            saved = process_deltas(deltas, cur_pg_check)
            print(f"Done: upserted {saved} row(s) into {POSTGRES_TARGET_TABLE}")
            count += 1
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