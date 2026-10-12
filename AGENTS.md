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
- Airflow DAG `postgres_to_s3` (`docker/airflow/dags/pipeline.py`) full-loads every table to
  `s3://$S3_BUCKET/<table>/<ds>/<table>.csv` (e.g. `menu/2026-10-10/menu.csv`); manual trigger
  only (`schedule=None`). The same logic runs standalone via
  `python docker/airflow/dags/export_tables.py --ds <date> [--only <table>]`.
