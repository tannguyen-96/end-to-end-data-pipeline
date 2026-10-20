import os

from dotenv import load_dotenv
import psycopg2

load_dotenv()


def main():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        sslmode=os.getenv("POSTGRES_SSLMODE", "prefer"),
    )
    # REINDEX INDEX CONCURRENTLY cannot run inside a transaction block.
    conn.autocommit = True
    cur = conn.cursor()
    try:
        print("REINDEX INDEX CONCURRENTLY review_embeddings_embedding_hnsw")
        cur.execute("REINDEX INDEX CONCURRENTLY review_embeddings_embedding_hnsw")
        print("done")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
