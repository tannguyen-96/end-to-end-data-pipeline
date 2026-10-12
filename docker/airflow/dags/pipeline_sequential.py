from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import kafka_export

# Sequential (table-by-table) variant of kafka_to_s3.
#
# Why this exists: pipeline.py (kafka_to_s3) schedules all 7 tables as INDEPENDENT
# tasks, so they all drain at once -> up to ~30 partition worker processes -> the
# local machine OOMs (kafka + connect were OOM-killed mid-run). table-by-table
# DAG chains one task per table so only ONE table is drained at a time, and
# kafka_export.export_parallel itself caps fan-out at min(partitions, 2).
#
# Use this DAG on dev machines; keep pipeline.py only if you want concurrent
# tables AND have real RAM/CPU headroom.
with DAG(
    dag_id="kafka_to_s3_sequential",
    description="Table-by-table Kafka CDC -> S3 (one table at a time, bounded memory)",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["kafka", "s3", "cdc", "sequential"],
) as dag:
    consume = []
    for table in kafka_export.TABLES:
        consume.append(
            PythonOperator(
                task_id=f"consume_{table}",
                python_callable=kafka_export.export_parallel,
                # ds is the run's logical date -> the load_date partition folder.
                op_kwargs={"table": table, "ds": "{{ logical_date | ds }}"},
            )
        )

    # Strictly serial chain: menu -> orders -> users -> restaurant -> reviews -> food -> order_items
    for prev, next_ in zip(consume, consume[1:]):
        prev >> next_