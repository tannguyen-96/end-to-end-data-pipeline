from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.macros import ds_add

import kafka_export

DBT = "/opt/airflow/dbt_venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/zomato"

SNOWFLAKE_CONN_ID = 'snowflake_default' # Snowflake default connection ID for Airflow
S3_BUCKET = os.environ["S3_BUCKET"]
SQL_DIR = Path(__file__).parent / "sql"
LOAD_DATE = "2026-10-15" # Test
# LOAD_DATE = datetime.now().strftime('%Y-%m-%d')

def _read_sql(name: str) -> str:
    return (SQL_DIR / name).read_text(encoding="utf-8")


with DAG(
    dag_id="kafka_to_s3",
    description="End-to-end: Debezium CDC -> S3 Parquet -> Snowflake (COPY INTO _raw + MERGE)",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["kafka", "s3", "cdc", "snowflake", "merge"]
) as dag:
    # Version 19
    # landing_tasks = []
    # for table in kafka_export.TABLES:
    # # for table in [t for t in kafka_export.TABLES if t == "food"]:
    #     consume = PythonOperator(
    #         task_id=f"consume_{table}_to_s3",
    #         python_callable=kafka_export.export_parallel,
    #         op_kwargs={"table": table, "ds": LOAD_DATE},
    #     )

    #     wait_for_files = S3KeySensor(
    #         task_id=f"wait_{table}",
    #         bucket_name=S3_BUCKET,
    #         bucket_key=f"{table}/load_date={LOAD_DATE}/{table}-*.parquet",
    #         wildcard_match=True,
    #         aws_conn_id=None,
    #         # timeout=60 * 60,
    #         timeout=60 * 5, # Test
    #         poke_interval=60, # check every 60 seconds
    #         mode="reschedule",
    #     )

    #     copy_into_landing = SQLExecuteQueryOperator(
    #         task_id=f"merge_{table}_to_snowflake",
    #         conn_id=SNOWFLAKE_CONN_ID,
    #         sql=_read_sql(f"LANDING.{table}.sql"),
    #         split_statements=True,
    #         autocommit=True,
    #         params={"load_date": LOAD_DATE},
    #     )

    #     landing_tasks.append(copy_into_landing)
    #     consume >> wait_for_files >> copy_into_landing

    # Run dbt raw, only after EVERY table has finished landing in Snowflake.
    dbt_build_raw = BashOperator(
        task_id="dbt_build_raw",
        bash_command=f"{DBT} run --select raw --project-dir {DBT_PROJECT} --profiles-dir {DBT_PROJECT}",
    )
    
    # silver_technical_tests = BashOperator(
    #     task_id="silver_technical_tests",
    #     cwd="/opt/airflow/walmart_project",
    #     bash_command="dbt test --select silver_technical"
    # )

    # landing_tasks >> dbt_build_raw
    dbt_build_raw