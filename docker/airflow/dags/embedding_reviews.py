import os
import json
import snowflake.connector as conn
from dotenv import load_dotenv
import requests
import time

load_dotenv()

# Embedding models
# Pay-as-you-go (OpenAI) -> returns 1536 dims, does NOT match the 768-dim table
# MODEL = "text-embedding-3-small"
# API_KEY = os.getenv("OPENAI_API_KEY")
# BASE_URL = "https://api.openai.com/v1"

# Local Ollama -> nomic-embed-text returns 768 dims and matches REVIEW_EMBEDDINGS VECTOR(FLOAT, 768).
# Pull it once on the host: `ollama pull nomic-embed-text`
MODEL = os.getenv("EMBED_MODELS", "nomic-embed-text")
BASE_URL = os.getenv("EMBED_BASE_URL", "http://host.docker.internal:11434")
# BASE_URL = "http://localhost:11434"

# Must match REVIEW_EMBEDDINGS.EMBEDDING -> VECTOR(FLOAT, 768)
EMBED_DIM = int(os.getenv("EMBED_DIM", "768"))

BATCH = 1000
CHUNK_SIZE = 100

# BATCH = 50 # Test
# CHUNK_SIZE = 5 # Test

TARGET_TABLE = "ZOMATO.AI.REVIEW_EMBEDDINGS"
SOURCE_TABLE = "ZOMATO.STAGING.STG_REVIEWS"

def get_connection():
    return conn.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "ZOMATO_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE", "ZOMATO")
    )


def create_output_table(cursor):
    cursor.execute("CREATE SCHEMA IF NOT EXISTS ZOMATO.AI")
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            REVIEW_ID NUMBER(38,0),
            ORDER_ID NUMBER(38,0),
            RESTAURANT_ID NUMBER(38,0),
            RATING NUMBER(38,0),
            COMMENT STRING,
            ROW_HASH STRING,
            EMBEDDING VECTOR(FLOAT, {EMBED_DIM}),
            EMBEDDING_MODEL STRING,
            EMBEDDED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)


def get_reviews_to_embed(cursor):
    cursor.execute(f"""
        SELECT R.REVIEW_ID, R.ORDER_ID, R.RESTAURANT_ID, R.RATING, R.COMMENT,
               HASH(R.COMMENT) AS ROW_HASH
        FROM {SOURCE_TABLE} R
        LEFT JOIN {TARGET_TABLE} RE ON R.REVIEW_ID = RE.REVIEW_ID
        WHERE RE.REVIEW_ID IS NULL
        OR RE.ROW_HASH != HASH(R.COMMENT)
        LIMIT {BATCH}
    """)
    return cursor.fetchall()


def embed_review(comment, review_id, maxRetry=3, backoffRate=2):
    for attempt in range(maxRetry):
        try:
            response = requests.post(f"{BASE_URL}/api/embed", timeout=60, json={
                "model": MODEL,
                "input": comment
            })
            # Ollama /api/embed -> {"embeddings": [[...]]}; older /api/embeddings -> {"embedding": [...]}
            payload = response.json()
            embeddings = payload.get("embeddings")
            embedding = embeddings[0] if embeddings else payload["embedding"]
            print(f"Model {MODEL} embedded review. Review ID: {review_id} ({len(embedding)} dims)")
            return embedding
        except Exception as e:
            if attempt == maxRetry - 1:
                raise e
            delay = backoffRate ** attempt
            print(f"Attempt {attempt + 1} failed for model {MODEL}: {e} (retrying in {delay}s)")
            time.sleep(delay)
            continue
    return None


def save_results(cursor, rows):
    if not rows:
        return
    try:
        cursor.execute("BEGIN")
        cursor.executemany(f"""
            MERGE INTO {TARGET_TABLE} AS t
            USING (
                SELECT %s AS REVIEW_ID, %s AS ORDER_ID, %s AS RESTAURANT_ID, %s AS RATING,
                       %s AS COMMENT, %s AS ROW_HASH,
                       PARSE_JSON(%s)::VECTOR(FLOAT, {EMBED_DIM}) AS EMBEDDING, %s AS EMBEDDING_MODEL
            ) AS s
            ON t.REVIEW_ID = s.REVIEW_ID
            WHEN MATCHED THEN UPDATE SET
                t.ORDER_ID        = s.ORDER_ID,
                t.RESTAURANT_ID   = s.RESTAURANT_ID,
                t.RATING          = s.RATING,
                t.COMMENT         = s.COMMENT,
                t.ROW_HASH        = s.ROW_HASH,
                t.EMBEDDING       = s.EMBEDDING,
                t.EMBEDDING_MODEL = s.EMBEDDING_MODEL,
                t.EMBEDDED_AT     = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (REVIEW_ID, ORDER_ID, RESTAURANT_ID, RATING, COMMENT, ROW_HASH,
                 EMBEDDING, EMBEDDING_MODEL, EMBEDDED_AT)
            VALUES (s.REVIEW_ID, s.ORDER_ID, s.RESTAURANT_ID, s.RATING, s.COMMENT, s.ROW_HASH,
                    s.EMBEDDING, s.EMBEDDING_MODEL, CURRENT_TIMESTAMP())
        """, rows)
        cursor.execute("COMMIT")
    except Exception:
        cursor.execute("ROLLBACK")
        raise


def process_batch(cursor, reviews):
    print(f"Embedding {len(reviews)} reviews...")
    buffer, saved, failed = [], 0, 0

    # with open("embeddings.md", "w") as f:
    #     f.write("")
    for review_id, order_id, restaurant_id, rating, comment, row_hash in reviews:
        try:
            embedding = embed_review(comment, review_id)
            buffer.append((
                review_id,
                order_id,
                restaurant_id,
                rating,
                comment,
                row_hash,
                json.dumps(embedding),
                MODEL
            ))
            # with open("embeddings.md", "a") as f:
            #     f.write(f"## Review ID: {review_id}\n")
            #     f.write(f"- Comment: {comment}\n")
            #     f.write(f"- Model: {MODEL}\n")
            #     f.write(f"- Dims: {len(embedding)}\n")
            #     f.write(f"- Embedding: {json.dumps(embedding)}\n")
            #     f.write(f"----------------------------------\n")
        except Exception as e:
            failed += 1
            print(f"Failed to embed review {review_id}: {e}")

        if len(buffer) >= CHUNK_SIZE:
            save_results(cursor, buffer)
            saved += len(buffer)
            print(f"Saved {saved} embedded reviews...")
            buffer = []
    if buffer:
        save_results(cursor, buffer)
        saved += len(buffer)
        print(f"Saved {saved} embedded reviews...")

    print(f"Done: {saved} saved, {failed} failed")


def main():
    # return
    connection = None
    cursor = None
    try:
        connection = get_connection()
        cursor = connection.cursor()
        create_output_table(cursor)

        deadline = time.monotonic() + 5*60  # 5 minutes from now
        count = 0

        # while time.monotonic() < deadline:
        while count < 1:  # end after 1 batch
            reviews = get_reviews_to_embed(cursor)

            if len(reviews) == 0:
                print("No new reviews to embed.")
                break

            process_batch(cursor, reviews)
            count += 1
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


if __name__ == "__main__":
    main()
