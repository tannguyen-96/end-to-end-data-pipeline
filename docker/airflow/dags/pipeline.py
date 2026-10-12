from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import kafka_export

with DAG(
    dag_id="kafka_to_s3",
    description="Consume Debezium CDC topics -> S3 Parquet (s3://<bucket>/<table>/load_date=<ds>/)",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["kafka", "s3", "cdc"],
) as dag:
    # attempt 5
    for table in kafka_export.TABLES:
        PythonOperator(
            task_id=f"consume_{table}",
            python_callable=kafka_export.export_parallel,
            # ds is the run's logical date -> the load_date partition folder.
            op_kwargs={"table": table, "ds": "{{ logical_date | ds }}"},
        )