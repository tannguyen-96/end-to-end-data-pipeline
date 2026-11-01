"""Shared Postgres connection helper for the Postgres-native AI jobs.

The Snowflake versions of the enrichment/embedding jobs keep their own
connections; this module only serves enrich_reviews_pg.py and
embedding_reviews_pg.py, which read public.reviews directly and write
public.review_enriched / public.review_embeddings.
"""
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def get_connection(register_vector_type: bool = False):
    """Open a psycopg2 connection from the shared POSTGRES_* env vars.

    register_vector_type=True installs the pgvector adapter/typecaster, which
    lets a plain Python list be passed as a vector parameter. Only the
    embedding job needs it.
    """
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        sslmode=os.getenv("POSTGRES_SSLMODE", "prefer"),
    )
    if register_vector_type:
        # Imported lazily so the enrichment job does not need pgvector installed.
        from pgvector.psycopg2 import register_vector

        register_vector(conn)
    return conn
