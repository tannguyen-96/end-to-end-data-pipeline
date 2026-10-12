# end-to-end-data-pipeline

Food-delivery data pipeline: **Postgres → Debezium CDC → Kafka → S3 (Parquet) → Snowflake**.

```
            Debezium (Kafka Connect)               Airflow 3.x
┌──────────┐  CDC snapshot+stream   ┌────────┐     ┌──────────────────────┐
│ Postgres │ ─────────────────────► │ Kafka  │ ──► │ kafka_to_s3[_seq]    │
│zomato-   │  topics: pg.public.*   │ 3.9.0  │     │ drain topics → S3    │
│source    │                        │(1 node)│     │ Parquet (chunked,    │
└──────────┘                        └────────┘     │ bounded memory)      │
                                                   └──────────┬───────────┘
                                                              ▼
                                                   ┌──────────────────────┐
                                                   │ S3                   │
                                                   │ <table>/load_date=<ds>│
                                                   └──────────┬───────────┘
                                                              ▼  (TODO2)
                                                   ┌──────────────────────┐
                                                   │ Snowflake            │
                                                   └──────────────────────┘
```

## Layout

- `data/` — source CSVs, `ddl.sql`, importer/verifier, `ERD.md`.
- `docker/postgres/` — pipeline Postgres (container `zomato-source`, localhost:5432).
- `docker/kafka-debezium/` — Kafka 3.9.0 broker, Kafka Connect 3.6.2 with Debezium
  PostgreSQL connector (`zomato-source`), kafka-ui (localhost:8080, view topics/offsets),
  `connect-init`/`zomato-source.json` (connector config).
- `docker/airflow/` — Airflow 3.x image + DAGs (`docker/airflow/dags/`).
- `docker/pgadmin/` — pgAdmin connected to the pipeline Postgres.
- `snowflake/`, `src/` — Snowflake setup and package scaffold.
- `.env` (repo root, not committed) — Postgres, Kafka, S3, Airflow credentials.
- `AGENTS.md` — verified commands + conventions (stacks are independent compose projects).

## Verified commands

Docker — each stack is independent (own compose file, network, volume):

```sh
stack start | stop | down | restart | status | build   [postgres|airflow|pgadmin|kafka-debezium|all]
stack logs <stack>

# or raw compose from a stack dir (stack passes --env-file ../../.env)
cd docker/airflow && docker compose --env-file ../../.env up -d          # localhost:8080 (admin/admin)
cd docker/kafka-debezium && docker compose --env-file ../../.env up -d   # UI localhost:8080, connect localhost:8083
```

Data — with the repo venv:

```sh
python data/import_data.py --create           # DDL + load all 7 tables (~34M rows, ~4 GB)
python data/import_data.py --only <table>     # load one table
python data/verify_data.py --db               # compare CSV row counts vs the DB
```

## Airflow DAGs (docker/airflow/dags/)

| DAG | File | Behavior | Use when |
|---|---|---|---|
| `kafka_to_s3` | `pipeline.py` | All 7 tables as **independent tasks run in parallel** | Dedicated box with RAM/CPU headroom |
| `kafka_to_s3_sequential` | `pipeline_sequential.py` | **One table at a time** (`max_active_tasks=1`, chained tasks) | **Dev machine — recommended** |

Manual trigger only (`schedule=None`). The export logic is table-agnostic; the DAGs
differ only in *how many tables run concurrently*.

```sh
docker exec airflow-scheduler-1 airflow dags trigger kafka_to_s3_sequential
```

Both call `kafka_export.export_parallel(table, ds)`; `ds` is the run's logical date
and becomes the S3 `load_date=<ds>` folder.

## Kafka → S3 exporter design (`kafka_export.py`)

Per-partition worker (process per partition, fan-out capped at `min(partitions, 2)` per table):

```
            poll (≤ CHUNK_MAX_RECORDS / poll)
  reader ──────────────────────────────────► reader accumulates a chunk
  thread   (records AND wire bytes)           └─► queue.Queue(maxsize=QUEUE_SIZE)   ← bounded
                                                    ▲ put(block=True) = BACKPRESSURE
                                                    └── UPLOAD_WORKERS uploader threads
                                                          → S3 Parquet (retry w/ backoff)
                                                          → report chunk end offset
                                          reader then commits only the contiguous prefix
```

Properties:

- **Bounded memory, independent of topic size.** Chunks are capped by
  `CHUNK_MAX_RECORDS` **and** `CHUNK_MAX_BYTES`; the reader blocks when the queue is
  full so it can never out-run the uploaders.
  Peak per partition ≈ `(QUEUE_SIZE + UPLOAD_WORKERS + 1) × CHUNK_MAX_BYTES`
  (defaults: 2+2+1 = 5 × 32 MB ≈ 160 MB per partition process).
- **At-least-once.** Offsets are committed only after the chunk's S3 upload succeeds,
  and only for the *contiguous* prefix (a failed chunk never commits past itself).
- **Retries overwrite, they don’t duplicate.** S3 keys embed the offset range:
  `s3://<bucket>/<table>/load_date=<ds>/<table>-p<N>-o<start>-<end>.parquet`
  → retrying a chunk rewrites the same object. Prefix + Parquet schema unchanged.
- **Exponential backoff** on S3 failures; after `UPLOAD_MAX_RETRIES` the task fails
  without committing.

Config (env vars, all optional):

| Var | Default | Meaning |
|---|---|---|
| `CHUNK_MAX_RECORDS` | 50_000 | records per chunk |
| `CHUNK_MAX_BYTES` | 32_000_000 | wire bytes per chunk |
| `QUEUE_SIZE` | 2 | bounded queue capacity (chunks) |
| `UPLOAD_WORKERS` | 2 | upload threads per partition |
| `UPLOAD_MAX_RETRIES` | 5 | max attempts per chunk |
| `UPLOAD_BACKOFF_BASE_S` | 1 | backoff = base × 2^attempt |
| `IDLE_TIMEOUT_MS` | 10_000 | stop once the topic is idle this long |

## How this changed when things broke (issues → fixes)

The current design is the result of real failures — tech/solution changes as issues appear:

1. **Parallel per-partition export with `pool.map` arg-order bug** → all tasks failed
   (string partition couldn't resolve a leader). Fix: ordered iterables as
   `(table, partition, ds)`.
2. **Whole-partition buffering in RAM** → host OOM; the kernel OOM-killed Kafka and
   Connect mid-run (both `Exited(137)`), all tasks died. Fix: chunked write + commit
   per slice.
3. **All 7 tables as independent parallel tasks** → still too much for a dev machine.
   Fix: bounded-queue streaming + `min(partitions, 2)` fan-out cap, and the
   sequential **`kafka_to_s3_sequential`** DAG to run one table at a time.

## TODO (TODO.txt)

- **TODO1 — DONE**: Postgres source → CDC Debezium → Kafka → Airflow DAG Kafka → S3.
- **TODO2 — next**: merge the partitioned S3 folders (`load_date=<ds>`) into Snowflake.