"""Postgres-native AI DAG (Snowflake free tier retired).

Runs the two Postgres AI jobs in order:
    enrich_reviews_pg   -> public.review_enriched
    embedding_reviews_pg -> public.review_embeddings (pgvector)
    reindex_embeddings_hnsw -> rebuild the HNSW index after the bulk fill

It has no Snowflake, S3, or dbt dependency; kafka_to_s3/pipeline.py is left
untouched. Manual trigger only.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import embedding_reviews_pg
import enrich_reviews_pg
import reindex_embeddings_hnsw

with DAG(
    dag_id="ai_enrich_embed_pg",
    description="Local AI jobs: enrich reviews + embed reviews straight into Postgres",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["postgres", "llm", "embeddings", "pgvector"],
) as dag:
    enrich = PythonOperator(
        task_id="enrich_reviews_pg",
        python_callable=enrich_reviews_pg.main,
    )

    embed = PythonOperator(
        task_id="embedding_reviews_pg",
        python_callable=embedding_reviews_pg.main,
    )

    reindex = PythonOperator(
        task_id="reindex_embeddings_hnsw",
        python_callable=reindex_embeddings_hnsw.main,
    )

    enrich >> embed >> reindex
