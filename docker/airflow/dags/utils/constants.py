import os

# ==========================================
# S3
# ==========================================
S3_BUCKET = os.environ["S3_BUCKET"]

# ==========================================
# DBT
# ==========================================
DBT = "/opt/airflow/dbt_venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/zomato"