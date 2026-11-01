"""Postgres-native review enrichment (Snowflake free tier retired).

Mirrors enrich_reviews.py, but:
  * reads public.reviews directly (no STG_REVIEWS / Snowflake hop),
  * labels each DISTINCT comment once and fans the result out to every review
    that shares the text (300k reviews -> 294 distinct comments in this dataset),
  * stores one row per review in public.review_enriched,
  * keeps a comment-hash cache in public.review_enrichment_cache so reruns and
    duplicate comments cost zero LLM calls.

The Snowflake implementation is left untouched in enrich_reviews.py.
"""
import json
import os
import time

import requests
from dotenv import load_dotenv

from utils.pg import get_connection

load_dotenv()

# ==========================================
# CONFIG
# ==========================================
MODEL = os.getenv("ENRICH_MODEL", "qwen2.5:3b")
BASE_URL = os.getenv("ENRICH_BASE_URL", "http://host.docker.internal:11434")

# Distinct comments per LLM round; each round is followed by a cache fan-out.
BATCH = int(os.getenv("ENRICH_BATCH_SIZE", "500"))
# 0 = drain until every review is enriched (runaway guard otherwise).
MAX_BATCHES = int(os.getenv("ENRICH_MAX_BATCHES", "0"))

# Runaway guards. Use the values below for a quick smoke test.
# BATCH = 50
# MAX_BATCHES = 1

TOPICS = ["food quality", "delivery", "pricing", "service", "packaging", "other"]
SENTIMENT_LABELS = ["POSITIVE", "NEGATIVE", "NEUTRAL"]

SOURCE_TABLE = "public.reviews"
TARGET_TABLE = "public.review_enriched"
CACHE_TABLE = "public.review_enrichment_cache"

SCHEMA = {
    "type": "object",
    "properties": {
        "sentiment_label": {"type": "string"},
        "sentiment_score": {"type": "number"},
        "topic": {"type": "string"},
        "key_issue": {"type": ["string", "null"]},
    },
    "required": ["sentiment_label", "sentiment_score", "topic", "key_issue"],
}

SYSTEM_PROMPT = f"""
You classify customer reviews for a food delivery app.

For the review you are given, return:
- sentiment_label: positive, negative, or neutral
- sentiment_score: a number between -1.0 and 1.0
- topic: one of {TOPICS}
- key_issue: a short phrase of 6 words or less that describes the main issue in the review, if any. If there is no issue, return empty string
"""


# ==========================================
# LLM
# ==========================================
def classify_review(comment, comment_hash, maxRetry=3, backoffRate=2):
    """Ask the local Ollama model to label one comment."""
    for attempt in range(maxRetry):
        try:
            response = requests.post(
                f"{BASE_URL}/api/chat",
                timeout=60,
                json={
                    "model": MODEL,
                    "options": {"temperature": 0},
                    "stream": False,
                    "format": SCHEMA,
                    "messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": comment},
                    ],
                },
            )
            result = json.loads(response.json()["message"]["content"])
            print(f"Model {MODEL} enriched comment {comment_hash[:10]}")
            return result
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


def normalize_labels(labels):
    """Map model output onto the casing/ranges the dbt marts expect."""
    label = str(labels.get("sentiment_label") or "").strip().upper()
    if label not in SENTIMENT_LABELS:
        label = "NEUTRAL"

    try:
        score = float(labels.get("sentiment_score"))
    except (TypeError, ValueError):
        score = 0.0
    score = max(-1.0, min(1.0, score))

    topic = str(labels.get("topic") or "").strip().lower()
    if topic not in TOPICS:
        topic = "other"

    key_issue = labels.get("key_issue")
    key_issue = key_issue.strip() if isinstance(key_issue, str) else None
    if not key_issue:
        key_issue = None

    return label, score, topic, key_issue


# ==========================================
# DATABASE
# ==========================================
def get_comments_to_enrich(cursor):
    """Distinct comments that still need an LLM call (cache miss only).

    Reviews already covered by review_enriched (same md5) or by an existing
    cache entry are excluded, so this returns [] once everything is filled.
    """
    cursor.execute(
        f"""
        SELECT md5(r.comment) AS comment_hash, r.comment
        FROM {SOURCE_TABLE} r
        LEFT JOIN {TARGET_TABLE} e
            ON e.review_id = r.review_id
        WHERE r.comment IS NOT NULL
          AND btrim(r.comment) <> ''
          AND (e.review_id IS NULL OR e.row_hash IS DISTINCT FROM md5(r.comment))
          AND NOT EXISTS (
              SELECT 1
              FROM {CACHE_TABLE} c
              WHERE c.comment_hash = md5(r.comment)
          )
        GROUP BY md5(r.comment), r.comment
        LIMIT %s
        """,
        (BATCH,),
    )
    return cursor.fetchall()


def save_cache(cursor, rows):
    """Upsert newly classified comments into the cache."""
    if not rows:
        return 0
    cursor.executemany(
        f"""
        INSERT INTO {CACHE_TABLE}
            (comment_hash, sentiment_label, sentiment_score, topic, key_issue, model)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (comment_hash) DO UPDATE SET
            sentiment_label = EXCLUDED.sentiment_label,
            sentiment_score = EXCLUDED.sentiment_score,
            topic           = EXCLUDED.topic,
            key_issue       = EXCLUDED.key_issue,
            model           = EXCLUDED.model,
            created_at      = now()
        """,
        rows,
    )
    return len(rows)


def fan_out_from_cache(cursor):
    """Copy cached labels onto every review with the same comment hash.

    One rounded set-based statement instead of one round trip per review.
    """
    cursor.execute(
        f"""
        INSERT INTO {TARGET_TABLE}
            (review_id, sentiment_label, sentiment_score, topic, key_issue, model, row_hash, enriched_at)
        SELECT r.review_id, c.sentiment_label, c.sentiment_score, c.topic,
               c.key_issue, c.model, md5(r.comment), now()
        FROM {SOURCE_TABLE} r
        JOIN {CACHE_TABLE} c
            ON c.comment_hash = md5(r.comment)
        LEFT JOIN {TARGET_TABLE} e
            ON e.review_id = r.review_id
        WHERE r.comment IS NOT NULL
          AND btrim(r.comment) <> ''
          AND (e.review_id IS NULL OR e.row_hash IS DISTINCT FROM md5(r.comment))
        ON CONFLICT (review_id) DO UPDATE SET
            sentiment_label = EXCLUDED.sentiment_label,
            sentiment_score = EXCLUDED.sentiment_score,
            topic           = EXCLUDED.topic,
            key_issue       = EXCLUDED.key_issue,
            model           = EXCLUDED.model,
            row_hash        = EXCLUDED.row_hash,
            enriched_at     = EXCLUDED.enriched_at
        WHERE {TARGET_TABLE}.row_hash IS DISTINCT FROM EXCLUDED.row_hash
        """
    )
    return cursor.rowcount


def count_enriched(cursor):
    cursor.execute(f"SELECT count(*) FROM {TARGET_TABLE}")
    return cursor.fetchone()[0]


# ==========================================
# MAIN
# ==========================================
def main():
    connection = None
    cursor = None
    try:
        connection = get_connection()
        cursor = connection.cursor()

        batches = 0
        cached = 0
        failed = 0
        rows_written = 0

        while MAX_BATCHES == 0 or batches < MAX_BATCHES:
            # Cached comments may cover reviews never fanned out (e.g. a crash
            # between cache write and fan-out), so always drain the cache first.
            rows_written += fan_out_from_cache(cursor)
            connection.commit()

            comments = get_comments_to_enrich(cursor)
            if not comments:
                print("No new comments to enrich.")
                break

            print(f"Enriching {len(comments)} distinct comment(s)...")
            fresh = []
            for comment_hash, comment in comments:
                try:
                    result = classify_review(comment, comment_hash)
                    label, score, topic, key_issue = normalize_labels(result)
                    fresh.append((comment_hash, label, score, topic, key_issue, MODEL))
                except Exception as e:
                    failed += 1
                    print(f"Failed to classify comment {comment_hash[:10]}: {e}")

            if not fresh:
                # Nothing succeeded: stop instead of looping on the same batch.
                print("No comment succeeded in this batch; stopping.")
                break

            cached += save_cache(cursor, fresh)
            connection.commit()

            # Fan the new labels out immediately so the next query shrinks.
            rows_written += fan_out_from_cache(cursor)
            connection.commit()

            batches += 1
            print(
                f"Batch {batches}: {cached} comment(s) cached, "
                f"{rows_written} review row(s) written so far, {failed} failure(s)"
            )

        total = count_enriched(cursor)
        print(
            f"Done: {cached} comment(s) newly cached, {failed} failure(s), "
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
