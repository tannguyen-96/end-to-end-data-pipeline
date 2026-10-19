# AGENTS.md

## What this repo is

End-to-end food-delivery data pipeline. Raw CSVs in `data/` are loaded into a local
Dockerized Postgres, exported to S3, and loaded into Snowflake (Airflow orchestrates).

## Layout

- `data/` — source CSVs, `ddl.sql`, importer/verifier, `ERD.md`.
- `docker/postgres/` — pipeline Postgres, container `zomato-source` (replaces the old hosted Supabase).
- `docker/airflow/` — Airflow 3.x image + DAGs (`docker/airflow/dags/`).
- `docker/pgadmin/` — pgAdmin connected to the pipeline Postgres.
- `snowflake/`, `src/` — Snowflake setup and package scaffold.
- `opencode.json` — agent config, incl. the local Postgres MCP server.

## Verified commands

Docker — each stack is independent (own compose file, network, and volume):

```sh
# one command from the repo root (stack.ps1 / stack.cmd wrap compose + the root .env)
stack start | stop | down | restart | status | build   [postgres|airflow|pgadmin|all]
stack logs <stack>          # e.g. stack start, stack stop postgres, stack logs airflow

# raw equivalent — run from the stack dir (stack passes --env-file for you)
cd docker/postgres && docker compose --env-file ../../.env up -d   # zomato-source, localhost:5432
cd docker/airflow  && docker compose --env-file ../../.env up -d   # http://localhost:8080  (admin/admin)
cd docker/pgadmin  && docker compose --env-file ../../.env up -d   # http://localhost:5050  (admin@local.dev/admin)
```

Data — run with the repo venv (`.venv\Scripts\python.exe` on Windows):

```sh
python data/import_data.py --create           # apply DDL + load all 7 tables (~34M rows, ~4 GB)
python data/import_data.py --only <table>     # load one table
python data/verify_data.py --db               # compare CSV row counts against the DB
```

## Conventions

- Credentials come from the repo-root `.env` (loaded with `python-dotenv`); it is not committed.
- Postgres env vars: `POSTGRES_HOST/PORT/DB/USER/PASSWORD` (`POSTGRES_SSLMODE`, default `prefer`).
- The loader **appends**; pass `--truncate` to reload a table cleanly (no upserts).
- Table names are the CSV stems; **no `FOREIGN KEY` constraints** — relationships are logical
  (see `data/ERD.md`).
- `data/*.csv` are ignored by git (too large).
- Airflow DAG `kafka_to_s3` (`docker/airflow/dags/pipeline.py`) drains Debezium CDC from Kafka to
  Parquet on S3 (`<table>/load_date=<ds>/<table>-p<partition>-o<start>-<end>.parquet`) via
  `docker/airflow/dags/kafka_export.py`, then runs dbt in order:
  `dbt_landing_test -> dbt_build_raw -> dbt_raw_test -> enrich_reviews -> dbt_build_staging
  -> dbt_staging_test -> dbt_build_marts -> dbt_marts_test`. Manual trigger only (`schedule=None`).
  `kafka_to_s3_sequential` is the same DAG with one table at a time (low-RAM dev machines).
- `enrich_reviews.py` (same dir) labels reviews with an LLM into `ZOMATO.AI.REVIEW_ENRICHED`
  (+ `REVIEW_CACHE` keyed by comment hash, so identical comments cost one call). Runs standalone
  with `python docker/airflow/dags/enrich_reviews.py`; knobs are `ENRICH_*` env vars
  (`ENRICH_WORKERS`, `ENRICH_BATCH_SIZE`, `ENRICH_BASE_URL`, `ENRICH_MODELS`). Local Ollama must
  listen on `0.0.0.0` (`OLLAMA_HOST`) for the container to reach it at `host.docker.internal:11434`.
  dbt consumes it as `source('ai', 'review_enriched')` -> `stg_review_enrichment` -> `fct_reviews`
  -> `mart_restaurant_sentiment`.
- `embedding_reviews.py` (same dir) mirrors `enrich_reviews.py` but embeds review comments with an
  embedding model into `ZOMATO.AI.REVIEW_EMBEDDINGS` (`EMBEDDING VECTOR(FLOAT, 768)`, MERGE on
  `REVIEW_ID`, re-embeds only when the comment hash changes). Runs standalone with
  `python docker/airflow/dags/embedding_reviews.py`; knobs are `EMBED_*` env vars
  (`EMBED_BASE_URL`, `EMBED_MODELS`, `EMBED_DIM`). Default model is local Ollama
  `nomic-embed-text` (768 dims, `ollama pull nomic-embed-text`), reachable at
  `host.docker.internal:11434`.
