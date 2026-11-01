"""Postgres-native review embeddings (Snowflake free tier retired).

Mirrors embedding_reviews.py, but:
  * reads public.reviews directly (no STG_REVIEWS / Snowflake hop),
  * embeds each DISTINCT comment once and fans the vector out to every review
    that shares the text (300k reviews -> 294 distinct comments in this dataset),
  * writes public.review_embeddings (pgvector, 768 dims) itself, so the old
    copy_embeddings_to_pg.py Snowflake -> Postgres step is no longer needed,
  * skips rows whose row_hash (md5(comment)) already matches, so reruns resume.

The Snowflake implementation is left untouched in embedding_reviews.py.
"""
import os
import time

import requests
from dotenv import load_dotenv

from utils.pg import get_connection

load_dotenv()

# ==========================================
# CONFIG
# ==========================================
# Local Ollama -> nomic-embed-text returns 768 dims and matches
# public.review_embeddings.EMBEDDING VECTOR(768).
# Pull it once on the host: `ollama pull nomic-embed-text`
MODEL = os.getenv("EMBED_MODEL") or os.getenv("EMBED_MODELS", "nomic-embed-text")
BASE_URL = os.getenv("EMBED_BASE_URL", "http://host.docker.internal:11434")

# Must match public.review_embeddings.embedding -> vector(768)
EMBED_DIM = int(os.getenv("EMBED_DIM", "768"))

# Distinct comments per round.
BATCH = int(os.getenv("EMBED_BATCH_SIZE", "500"))
# Commit after this many comments' fan-out (keeps transactions bounded).
CHUNK_COMMENTS = int(os.getenv("EMBED_CHUNK_COMMENTS", "25"))
# 0 = drain until every review is embedded (runaway guard otherwise).
MAX_BATCHES = int(os.getenv("EMBED_MAX_BATCHES", "0"))

# Runaway guards. Use the values below for a quick smoke test.
# BATCH = 50
# MAX_BATCHES = 1

SOURCE_TABLE = "public.reviews"
TARGET_TABLE = "public.review_embeddings"


# ==========================================
# LLM / EMBEDDING API
# ==========================================
def embed_review(comment, comment_hash, maxRetry=3, backoffRate=2):
    """Embed one comment with the local Ollama server."""
    for attempt in range(maxRetry):
        try:
            response = requests.post(
                f"{BASE_URL}/api/embed",
                timeout=60,
                json={"model": MODEL, "input": comment},
            )
            # Ollama /api/embed -> {"embeddings": [[...]]};
            # older /api/embeddings -> {"embedding": [...]}
            payload = response.json()
            embeddings = payload.get("embeddings")
            embedding = embeddings[0] if embeddings else payload["embedding"]

            if len(embedding) != EMBED_DIM:
                raise ValueError(
                    f"embedding dim mismatch: got {len(embedding)}, expected {EMBED_DIM}"
                )

            print(
                f"Model {MODEL} embedded comment {comment_hash[:10]} "
                f"({len(embedding)} dims)"
            )
            return embedding
        except Exception as e:
            if attempt == maxRetry - 1:
                raise e
            delay = backoffRate ** attempt
            print(
                f"Attempt {attempt + 1} failed for model {MODEL}: {e} "
                f"(retrying in {delay}s)"
            )
            time.sleep(delay)
    return None


# ==========================================
# DATABASE
# ==========================================
def get_comments_to_embed(cursor):
    """Distinct comments whose reviews are missing or stale in the sink."""
    cursor.execute(
        f"""
        SELECT md5(r.comment) AS comment_hash, r.comment
        FROM {SOURCE_TABLE} r
        LEFT JOIN {TARGET_TABLE} e
            ON e.review_id = r.review_id
        WHERE r.comment IS NOT NULL
          AND btrim(r.comment) <> ''
          AND (e.review_id IS NULL OR e.row_hash IS DISTINCT FROM md5(r.comment))
        GROUP BY md5(r.comment), r.comment
        LIMIT %s
        """,
        (BATCH,),
    )
    return cursor.fetchall()


def fan_out_embedding(cursor, comment_hash, embedding):
    """Write one comment's vector to every review that carries that comment.

    regenerate the vector only when the comment text changed, same guard the
    old Snowflake -> Postgres copy used.
    """
    cursor.execute(
        f"""
        INSERT INTO {TARGET_TABLE}
            (review_id, order_id, restaurant_id, rating, comment, row_hash,
             embedding, embedding_model, embedded_at)
        SELECT r.review_id, r.order_id, r.restaurant_id, r.rating, r.comment,
               md5(r.comment), %s::vector({EMBED_DIM}), %s, now()
        FROM {SOURCE_TABLE} r
        WHERE md5(r.comment) = %s
        ON CONFLICT (review_id) DO UPDATE SET
            order_id        = EXCLUDED.order_id,
            restaurant_id   = EXCLUDED.restaurant_id,
            rating          = EXCLUDED.rating,
            comment         = EXCLUDED.comment,
            row_hash        = EXCLUDED.row_hash,
            embedding       = EXCLUDED.embedding,
            embedding_model = EXCLUDED.embedding_model,
            embedded_at     = EXCLUDED.embedded_at
        WHERE {TARGET_TABLE}.row_hash IS DISTINCT FROM EXCLUDED.row_hash
        """,
        (embedding, MODEL, comment_hash),
    )
    return cursor.rowcount


def count_embedded(cursor):
    cursor.execute(f"SELECT count(*) FROM {TARGET_TABLE}")
    return cursor.fetchone()[0]


# ==========================================
# MAIN
# ==========================================
def main():
    connection = None
    cursor = None
    try:
        connection = get_connection(register_vector_type=True)
        cursor = connection.cursor()

        batches = 0
        embedded = 0
        failed = 0
        rows_written = 0
        pending_commit = 0

        while MAX_BATCHES == 0 or batches < MAX_BATCHES:
            comments = get_comments_to_embed(cursor)
            if not comments:
                print("No new comments to embed.")
                break

            print(f"Embedding {len(comments)} distinct comment(s)...")
            progress = 0
            for comment_hash, comment in comments:
                try:
                    embedding = embed_review(comment, comment_hash)
                except Exception as e:
                    failed += 1
                    print(f"Failed to embed comment {comment_hash[:10]}: {e}")
                    continue

                rows_written += fan_out_embedding(cursor, comment_hash, embedding)
                embedded += 1
                progress += 1
                pending_commit += 1

                if pending_commit >= CHUNK_COMMENTS:
                    connection.commit()
                    pending_commit = 0

            connection.commit()
            pending_commit = 0

            if progress == 0:
                # Nothing succeeded: stop instead of looping on the same batch.
                print("No comment succeeded in this batch; stopping.")
                break

            batches += 1
            print(
                f"Batch {batches}: {embedded} comment(s) embedded, "
                f"{rows_written} review row(s) written so far, {failed} failure(s)"
            )

        total = count_embedded(cursor)
        print(
            f"Done: {embedded} comment(s) embedded, {failed} failure(s), "
            f"{rows_written} review row(s) touched, {total} row(s) in {TARGET_TABLE}"
        )
    except Exception as e:
        if connection is not None:
            connection.rollback()
        print(f"An error occurred: {e}")
        raise
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    main()
