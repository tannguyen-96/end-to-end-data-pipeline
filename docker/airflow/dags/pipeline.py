from __future__ import annotations

from datetime import datetime
from pathlib import Path

# ==========================================
# AIRFLOW
# ==========================================
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.task.trigger_rule import TriggerRule

# ==========================================
# TASKS
# ==========================================
import embedding_reviews
import enrich_reviews
import copy_embeddings_to_pg
import reindex_embeddings_hnsw
import kafka_export

# ==========================================
# UTILS
# ==========================================
from utils.helpers import read_file

SQL_DIR = Path(__file__).parent / "sql"
LOAD_DATE = "{{ logical_date | ds }}"
DBT = "/opt/airflow/dbt_venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/zomato"

# ==========================================
# DAG
# ==========================================
with DAG(
    dag_id="kafka_to_s3",
    description="End-to-end: Debezium CDC -> S3 Parquet -> Snowflake (COPY INTO _raw + MERGE)",
    schedule=None,  # manual trigger only change to daily later
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["kafka", "s3", "cdc", "snowflake", "merge"]
) as dag:
    # Version 28
    landing_tasks = []
    for table in kafka_export.TABLES:
    # for table in [t for t in kafka_export.TABLES if t == "food"]:
        consume = PythonOperator(
            task_id=f"consume_{table}_to_s3",
            python_callable=kafka_export.export_parallel,
            op_kwargs={"table": table, "ds": LOAD_DATE},
        )

        copy_into_landing = SQLExecuteQueryOperator(
            task_id=f"merge_{table}_to_snowflake",
            conn_id='snowflake_default',  # Snowflake default connection ID for Airflow
            sql=read_file(SQL_DIR / f"LANDING.{table}.sql", typ='sql'),
            split_statements=True,
            autocommit=True,
        )

        landing_tasks.append(copy_into_landing)
        consume >> copy_into_landing

    # Run dbt raw, only after EVERY table has finished landing in Snowflake.
    dbt_landing_test = BashOperator(
        task_id="dbt_landing_test",
        bash_command=f"{DBT} test --select 'source:*' --project-dir {DBT_PROJECT} --profiles-dir {DBT_PROJECT}",
        trigger_rule=TriggerRule.NONE_FAILED,
    )
    
    dbt_build_raw = BashOperator(
        task_id="dbt_build_raw",
        bash_command=f"{DBT} run --select raw --project-dir {DBT_PROJECT} --profiles-dir {DBT_PROJECT}",
    )
    
    dbt_raw_test = BashOperator(
        task_id="dbt_raw_test",
        bash_command=f"{DBT} test --select raw --project-dir {DBT_PROJECT} --profiles-dir {DBT_PROJECT}",
    )

    dbt_build_staging = BashOperator(
        task_id="dbt_build_staging",
        bash_command=f"{DBT} run --select staging --project-dir {DBT_PROJECT} --profiles-dir {DBT_PROJECT}",
    )
    
    dbt_staging_test = BashOperator(
        task_id="dbt_staging_test",
        bash_command=f"{DBT} test --select staging --project-dir {DBT_PROJECT} --profiles-dir {DBT_PROJECT}",
    )
    
    enrich = PythonOperator(
        task_id="enrich_reviews",
        python_callable=enrich_reviews.main
    )

    embedding = PythonOperator(
        task_id="embedding_reviews",
        python_callable=embedding_reviews.main
    )

    copy_embeddings_to_postgres = PythonOperator(
        task_id="copy_embeddings_to_postgres",
        python_callable=copy_embeddings_to_pg.main,
    )

    reindex_embeddings_hnsw = PythonOperator(
        task_id="reindex_embeddings_hnsw",
        python_callable=reindex_embeddings_hnsw.main,
    )

    dbt_build_marts = BashOperator(
        task_id="dbt_build_marts",
        bash_command=f"{DBT} run --select marts --project-dir {DBT_PROJECT} --profiles-dir {DBT_PROJECT}",
    )

    dbt_marts_test = BashOperator(
        task_id="dbt_marts_test",
        bash_command=f"{DBT} test --select marts --project-dir {DBT_PROJECT} --profiles-dir {DBT_PROJECT}",
    )
    
    landing_tasks >> dbt_landing_test >> dbt_build_raw >> dbt_raw_test >> dbt_build_staging >> dbt_staging_test >> [enrich, embedding]
    embedding >> copy_embeddings_to_postgres >> reindex_embeddings_hnsw
    enrich >> dbt_build_marts >> dbt_marts_test