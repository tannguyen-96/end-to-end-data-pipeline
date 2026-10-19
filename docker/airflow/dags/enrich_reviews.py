import os
import json
import snowflake.connector as conn
from openai import OpenAI
from dotenv import load_dotenv
import requests
import time

load_dotenv()

# Pay-as-you-go models
# MODEL = "gpt-5.5"
# API_KEY = os.getenv("OPENAI_API_KEY")
# BASE_URL = "https://api.openai.com/v1"

# Free-tier with limited usage
# MODEL = "gemini-3.5-flash-lite"
# MODEL = "gemini-3.8-flash"
# API_KEY = os.getenv("GEMINI_API_KEY")
# BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"

# Local
MODEL = "qwen2.5:3b"
BASE_URL = os.getenv("ENRICH_BASE_URL", "http://host.docker.internal:11434")
# BASE_URL = "http://localhost:11434"


BATCH = 500
CHUNK_SIZE = 100

# BATCH = 50 # Test
# CHUNK_SIZE = 5 # Test

TOPICS = ["food quality", "delivery", "pricing", "service", "packaging", "other"]

# client = OpenAI(
#     base_url=BASE_URL,
#     api_key=API_KEY,
#     timeout=60
# )

TARGET_TABLE = "ZOMATO.AI.REVIEW_ENRICHED"
SOURCE_TABLE = "ZOMATO.STAGING.STG_REVIEWS"

SCHEMA = {
    "type": "object",
    "properties": {
        "sentiment_label": {"type": "string"},
        "sentiment_score": {"type": "number"},
        "topic": {"type": "string"},
        "key_issue": {"type": ["string", "null"]}
    },
    "required": ["sentiment_label", "sentiment_score", "topic", "key_issue"]
}

SYSTEM_PROMPT = f"""
You classify customer reviews for a food delivery app.

For the review you are given, return:
- sentiment_label: positive, negative, or neutral
- sentiment_score: a number between -1.0 and 1.0
- topic: one of {TOPICS}
- key_issue: a short phrase of 6 words or less that describes the main issue in the review, if any. If there is no issue, return empty string
"""

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
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ZOMATO.AI.REVIEW_ENRICHED (
            REVIEW_ID STRING,
            SENTIMENT_LABEL STRING,
            SENTIMENT_SCORE FLOAT,
            TOPIC STRING,
            KEY_ISSUE STRING,
            MODEL STRING,
            ROW_HASH STRING,
            ENRICHED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    # cursor.execute("ALTER TABLE ZOMATO.AI.REVIEW_ENRICHED ADD COLUMN IF NOT EXISTS ROW_HASH STRING")
    
def get_reviews_to_enrich(cursor):
    cursor.execute(f"""
        SELECT R.REVIEW_ID, R.COMMENT, HASH(R.COMMENT) AS ROW_HASH
        FROM {SOURCE_TABLE} R
        LEFT JOIN {TARGET_TABLE} RE ON R.REVIEW_ID = RE.REVIEW_ID
        WHERE RE.REVIEW_ID IS NULL
        OR RE.ROW_HASH != HASH(R.COMMENT)
        LIMIT {BATCH}
    """)
    return cursor.fetchall()

def classify_review(comment, review_id, maxRetry=3, backoffRate=2):
    for attempt in range(maxRetry):
        try:
            # response = client.chat.completions.create(
            #     model=model,
            #     temperature=0,
            #     response_format={"type": "json_object"},
            #     messages=[
            #         {"role": "system", "content": SYSTEM_PROMPT},
            #         {"role": "user", "content": comment}
            #     ]
            # )
            response = requests.post(f"{BASE_URL}/api/chat", timeout=60, json={
                "model": MODEL,
                "options": { "temperature": 0 },
                "stream": False,
                "format": SCHEMA,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": comment}
                ]
            })
            # print(f"Raw response from model {model}: {response.json()}")
            result = json.loads(response.json()["message"]["content"])
            result['model'] = MODEL
            print(f"Model {MODEL} successfully enriched the review. Review ID: {review_id}")
            return result
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
                SELECT %s AS REVIEW_ID, %s AS SENTIMENT_LABEL, %s AS SENTIMENT_SCORE,
                       %s AS TOPIC, %s AS KEY_ISSUE, %s AS MODEL, %s AS ROW_HASH
            ) AS s
            ON t.REVIEW_ID = s.REVIEW_ID
            WHEN MATCHED THEN UPDATE SET
                t.SENTIMENT_LABEL = s.SENTIMENT_LABEL,
                t.SENTIMENT_SCORE = s.SENTIMENT_SCORE,
                t.TOPIC           = s.TOPIC,
                t.KEY_ISSUE       = s.KEY_ISSUE,
                t.MODEL           = s.MODEL,
                t.ROW_HASH        = s.ROW_HASH,
                t.ENRICHED_AT     = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (REVIEW_ID, SENTIMENT_LABEL, SENTIMENT_SCORE, TOPIC, KEY_ISSUE,
                 MODEL, ROW_HASH, ENRICHED_AT)
            VALUES (s.REVIEW_ID, s.SENTIMENT_LABEL, s.SENTIMENT_SCORE, s.TOPIC,
                    s.KEY_ISSUE, s.MODEL, s.ROW_HASH, CURRENT_TIMESTAMP())
        """, rows)
        cursor.execute("COMMIT")
    except Exception:
        cursor.execute("ROLLBACK")
        raise
    
def process_batch(cursor, reviews):
    print(f"Enriching {len(reviews)} reviews...")
    buffer, saved, failed = [], 0, 0
    
    # with open("results.md", "w") as f:
    #     f.write("")
    for review_id, comment, row_hash in reviews:
        try:
            labels = classify_review(comment, review_id)
            buffer.append((
                review_id,
                labels["sentiment_label"],
                labels["sentiment_score"],
                labels["topic"],
                labels["key_issue"],
                labels["model"],
                row_hash
            ))
            # with open("results.md", "a") as f:
            #     f.write(f"## Review ID: {review_id}\n")
            #     f.write(f"- Comment: {comment}\n")
            #     f.write(f"- Sentiment Label: {labels['sentiment_label']}\n")
            #     f.write(f"- Sentiment Score: {labels['sentiment_score']}\n")
            #     f.write(f"- Topic: {labels['topic']}\n")
            #     f.write(f"- Key Issue: {labels['key_issue']}\n")
            #     f.write(f"- Model: {labels['model']}\n")
            #     f.write(f"----------------------------------\n")
        except Exception as e:
            failed += 1
            print(f"Failed to classify review {review_id}: {e}")
        
        if len(buffer) >= CHUNK_SIZE:
            save_results(cursor, buffer)
            saved += len(buffer)
            print(f"Saved {saved} enriched reviews...")
            buffer = []
    if buffer:
        save_results(cursor, buffer)
        saved += len(buffer)
        print(f"Saved {saved} enriched reviews...")
    
    print(f"Done: {saved} saved, {failed} failed")
    
def main():
    # return
    try:
        connection = get_connection()
        cursor = connection.cursor()
        create_output_table(cursor)
        
        deadline = time.monotonic() + 5*60  # 5 minutes from now
        count = 0
        
        # while time.monotonic() < deadline:
        while count < 1:  # end after 1 batch
            reviews = get_reviews_to_enrich(cursor)
            
            if len(reviews) == 0:
                print("No new reviews to enrich.")
                break
            
            process_batch(cursor, reviews)
            count += 1
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    main()