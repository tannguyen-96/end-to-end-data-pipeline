# TIMELINE — build log

Chronological record of how this pipeline was built: what I did, what broke, and what I
learned. Raw notes first; merged into `README.md` later.

Each step has the same shape:

```
What I did / Why / Commands / Result / Mistake / Fix / Lesson
```

---

## Step 1 — Requirement and reference documents

### What I did
Started from a written requirement: build an end-to-end food-delivery data pipeline that
treats Postgres as the application source of truth, streams changes with CDC to Kafka, lands
them as Parquet in S3, loads Snowflake, transforms with dbt, and adds LLM enrichment plus
embeddings for similarity search.

Two tools are enough for the whole local run: **Python** and **Docker**. Everything else
(Postgres, Kafka, Debezium, Airflow) runs in containers, one compose file per stack under
`docker/`.

### Result
Requirement understood, tooling fixed: Python (venv + `python-dotenv`) and Docker Desktop.

### Mistake
None.

### Lesson
None.

---

## Step 2 — Dataset: upload to Google Drive, count rows per table

### What I did
The source CSVs are too large for git (`data/*.csv` is gitignored), so they live in Google
Drive and are pulled into `data/` before any load step.

**Dataset link:** [https://drive.google.com/drive/folders/1S-gmg-hJcFDb5Ej71xNQqeId1XpwtcG0?usp=sharing](https://drive.google.com/drive/folders/1S-gmg-hJcFDb5Ej71xNQqeId1XpwtcG0?usp=sharing)

After downloading, verify the row count of every table before loading, so a truncated
download or a bad upload is caught early.

### Table row counts

| # | Table | CSV | Rows | CSV size |
| --- | --- | --- | ---: | ---: |
| 1 | `users` | `data/users.csv` | 100,000 | ~10.7 MiB |
| 2 | `restaurant` | `data/restaurant.csv` | 148,541 | ~44.6 MiB |
| 3 | `food` | `data/food.csv` | 371,561 | ~16.4 MiB |
| 4 | `menu` | `data/menu.csv` | 1,179,936 | ~62.8 MiB |
| 5 | `orders` | `data/orders.csv` | 10,000,000 | ~1.24 GiB |
| 6 | `order_items` | `data/order_items.csv` | 22,998,179 | ~911 MiB |
| 7 | `reviews` | `data/reviews.csv` | 300,000 | ~24.2 MiB |
|   | **Total** | | **35,098,217** | **~2.45 GiB** |

Counts come from `data/ERD.md` (verified against the CSVs). Sizes are on-disk measurements.

### Commands
```sh
# row count per CSV (header excluded)
python data/verify_data.py

# compare CSV counts against what is actually in Postgres
python data/verify_data.py --db
```

### Result
7 CSVs present in `data/`, ~35.1M rows, ~2.45 GiB on disk.

### Mistake
None.

### Lesson
Count first, load second. A row-count check costs seconds and catches a truncated download
that would otherwise fail hours into the load.

---

## Step 3 — Build Postgres v16 in Docker

### What I did
Stood up the application/source database as a Dockerized Postgres v16 container named
`zomato-source`. Configuration is not duplicated: the container reads **the repo-root `.env`**,
so user, password, database name and port have one source of truth.

Stack lives in `docker/postgres/` — own compose file, own volume.

**References:** [`data/ERD.md`](data/ERD.md) (tables, keys, CSV quirks) ·
[`docs/Postgres-CDC.md`](docs/Postgres-CDC.md) (WAL, logical replication).

### Commands
```sh
# start Postgres 16, env from the repo-root .env
cd docker/postgres
docker compose --env-file ../../.env up -d

# check
docker ps --filter name=zomato-source
docker logs zomato-source --tail 20
```

Connection: `localhost:5432`, database/user/password from `.env`
(`POSTGRES_HOST/PORT/DB/USER/PASSWORD`).

Key container settings:

- `env_file: ../../.env` — no credentials hardcoded in the compose file.
- healthcheck via `pg_isready`, using `$$` so the container shell expands the injected vars.

### Agent workflow — agent wrote the scripts, I reviewed them

I did not write `data/ddl.sql`, `data/import_data.py` or `data/verify_data.py`. I described what
I needed and let **opencode** write all three; my job was review.

```text
1. "Read the CSV headers in data/ and write data/ddl.sql"
   -> 7 tables, one PK each, menu uses composite (menu_id, f_id),
      relationships documented only, no FOREIGN KEY constraints
2. "Write data/import_data.py"
   -> drops the unnamed leading index column, snake_cases headers,
      '--' -> NULL in restaurant.rating, digits out of menu.price,
      --create / --only / --truncate flags
3. "Write data/verify_data.py"
   -> per-table CSV count vs SELECT COUNT(*), --db flag
4. "Run them"
   -> python data/import_data.py --create
   -> python data/verify_data.py --db
```

What I checked (not wrote):

- DDL: 7 tables, exactly one PK per table, `menu` composite key, zero `FOREIGN KEY`.
- Loader: index column dropped, headers snake_cased, the three dirty columns handled.
- Verify: 35,098,217 rows counted on both sides, PK uniqueness confirmed.

With current LLM models this whole layer — schema, loader, verifier — comes out in one pass
instead of a day of typing, and the scripts are more consistent than what I would have written by
hand. Review stays with me: the model cannot know which data quirks matter until it is told, and
the row-count check is the only proof the load actually worked.

### Result
`zomato-source` running, 7 tables created, ~35.1M rows loaded, CSV-vs-DB row counts match.

### Mistake
None.

### Lesson
Let the agent do the typing, keep the review on yourself. Prompts that reference files already
in the repo get far better output than prompts that re-explain the schema.

---

## Step 4 — Kafka

### What I did
Wrote `docker/kafka-debezium/docker-compose.yml` with four services:

- `kafka` — `apache/kafka:3.9.0`, KRaft mode (broker + controller in one node, no ZooKeeper),
  host port 9092.
- `connect` — `quay.io/debezium/connect:3.6.2.Final`, the Debezium worker; REST on 8083 and
  `POSTGRES_*` read from the repo-root `.env`, so the connector config can use `${env:...}`.
- `connect-init` — one-shot curl job that registers `zomato-source.json` only if the connector
  is not already there.
- `kafka-ui` — `provectuslabs/kafka-ui` at **http://localhost:8090** (host 8090, because 8080
  belongs to Airflow), so I can watch topics and messages arrive live and see the connector
  state without touching the CLI.

**References:** [`docs/Kafka-Debezium.md`](docs/Kafka-Debezium.md) · official docs:

- Apache Kafka — https://kafka.apache.org/documentation/
- Debezium connector for PostgreSQL — https://debezium.io/documentation/reference/stable/connectors/postgresql.html
- Debezium tutorial — https://debezium.io/documentation/reference/stable/tutorial.html
- Kafka UI — https://github.com/provectus/kafka-ui

### Commands
```sh
cd docker/kafka-debezium
docker compose --env-file ../../.env up -d

# broker         localhost:9092
# Connect REST   localhost:8083
# Kafka UI       http://localhost:8090
```

### Result
None.

### Mistake
None.

### Lesson
None.

---

## Step 5 — Airflow

### What I did
Built `docker/airflow` — a custom Airflow 3 image (`build: .`) with `LocalExecutor`, plus a
compose stack:

- `apiserver` — the UI/API, `http://localhost:8080` (admin / admin). Airflow 3 renamed
  `webserver` to `api-server`.
- `scheduler` and `dag-processor` — Airflow 3 split DAG parsing into its own process.
- `postgres:15` — Airflow's own metadata database, separate from the pipeline Postgres.
- `dags/` mounted from the repo, so editing `docker/airflow/dags/*.py` needs no rebuild.

**References:** [`docs/Kafka-Parallel-Export.md`](docs/Kafka-Parallel-Export.md) · official docs:

- Running Airflow in Docker — https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

### Commands
```sh
cd docker/airflow
docker compose --env-file ../../.env up -d

# UI
# http://localhost:8080   admin / admin
```

### Result
None.

### Mistake
None.

### Lesson
None.

---

## Step 6 — Mistake #1: connecting Postgres to Kafka/Debezium

The three stacks were up, but Postgres and Kafka were invisible to each other. Three separate
problems, found in this order.

### 6.1 Missing network — `cdc-net`

**Symptom:** the Debezium connector could not reach the database; host `zomato-source` did not
resolve.

**Cause:** each stack had its own default compose network. Containers only see peers attached to
the same network, so Postgres, Kafka and Airflow sat in three isolated islands.

**Fix:** create one shared network and attach every stack to it.

```sh
docker network create cdc-net
```

Each compose file then references it as an external network:

```yaml
services:
  postgres:
    networks: [cdc-net]

networks:
  cdc-net:
    external: true
```

Recreate the containers so they join the new network:

```sh
cd docker/postgres && docker compose --env-file ../../.env up -d --force-recreate
```

**Docs:** Docker networking — https://docs.docker.com/engine/network/ ·
Compose `networks` reference — https://docs.docker.com/reference/compose-file/networks/

**Lesson:** decide the network topology before starting the first stack. Adding a network later
means recreating containers, and any data written into a container-local path goes with them.

### 6.2 Missing `wal_level` in the database

**Symptom:** the database accepted connections but logical decoding was unavailable, so the
connector could not start streaming.

**Cause:** the container was created with default Postgres settings. `wal_level` defaults to
`replica`, which records enough WAL for crash recovery but not for logical decoding.

**Fix:** connect to the running database and change the parameter. I used
[Harlequin](https://harlequin.sh/docs/getting-started), a terminal SQL client:

```sh
uv tool install harlequin

harlequin -a postgres "postgresql://admin:admin@localhost:5432/postgres"
```

```sql
ALTER SYSTEM SET wal_level = 'logical';
```

`ALTER SYSTEM` writes to `postgresql.auto.conf` and needs a restart. Verify after:

```sh
docker restart zomato-source
```

```sql
SHOW wal_level;   -- logical
```

Only `wal_level` was changed. `max_replication_slots` and `max_wal_senders` stayed at their
defaults, which was enough for one connector.

> Credentials above are local-only (`admin` / `admin` on `localhost`) — fine to keep in this doc.

**Lesson:** the `command:` flags in `docker-compose.yml` are the durable fix. Changing a Postgres
parameter at runtime works, but the setting is invisible in the repo until it is moved into the
compose file.

### 6.3 pgAdmin container swapped for Harlequin

**What I did:** originally ran pgAdmin as a container just to run a few queries. Replaced it with
Harlequin — a terminal SQL client, no container, no port, no login screen.

```sh
uv tool install harlequin

harlequin -a postgres "postgresql://admin:admin@localhost:5432/postgres"
```

**Why:** the pgAdmin container cost RAM and a port to do what a single CLI command does, and the
terminal tool is faster to reach when the only goal is "run this one `ALTER SYSTEM`".

**Docs:** https://harlequin.sh/docs/getting-started · https://github.com/tconbeer/harlequin

**Lesson:** a GUI container for a one-off query is a bad trade on a laptop. Keep a lightweight
client and drop the stack.

### 6.4 Result validation — connector RUNNING

With `cdc-net` in place and `wal_level=logical` applied, the `zomato-source` connector registered
and started streaming. Kafka UI → **Kafka Connect**
(`http://localhost:8090/ui/clusters/local/connectors`):

- Connectors **1**, Failed Connectors **0**, Failed Tasks **0**
- `zomato-source` — Connect `debezium`, Type `SOURCE`, Plugin
  `io.debezium.connector.postgresql.PostgresConnector`
- Status **RUNNING**, **1 of 1** tasks running
- All 7 topics created: `pg.public.menu`, `pg.public.orders`, `pg.public.users`,
  `pg.public.restaurant`, `pg.public.reviews`, `pg.public.food`, `pg.public.order_items`

![Kafka UI — Debezium connector zomato-source RUNNING](docs/imgs/kafka-ui-debezium-connector.png)

**Lesson:** the connector list is the proof the network fix worked. Before `cdc-net` the connector
failed on host resolution; after it, one connector, zero failed tasks, seven topics.

---

## Step 7 — Airflow phase 1: Kafka → S3 export

### What I did
Wrote `kafka_export.py` and wired one `PythonOperator` per table into the DAG
(`consume_<table>_to_s3`), each calling `kafka_export.export_parallel(table, ds)`. The goal of this
step is only to **move the CDC events from Kafka to S3 as Parquet** — no Snowflake, no dbt, no
transformation.

Phase 1 kept it deliberately simple:

- every topic has **1 partition** (Debezium default), so there is nothing to fan out;
- **no chunk split** — the partition is drained into one list and written as one Parquet object;
- one offset commit, **only after the S3 write succeeds**;
- S3 key `<table>/load_date=<ds>/<table>-o<start>-<end>.parquet` (no partition suffix — there is
  only one partition).

**Why a date folder.** The job is meant to run once a day and CDC is incremental, so each run only
carries what changed since the last commit. Writing into `load_date=<ds>/` per run keeps the data
trackable (one prefix = one day), maintainable (re-run or delete a bad day without touching the
others, backfills land in the right folder) and idempotent (same `ds` overwrites the same key).

**Re-run is safe.** Because the commit happens only after the file lands, a failed run can simply
be re-run: the consumer resumes from the last committed offset, the same offsets produce the same
S3 key, and `put_object` overwrites it — no duplicate files.

Full write-up, both diagrams and the timings: [`docs/Kafka-Parallel-Export-v1.md`](docs/Kafka-Parallel-Export-v1.md).
Diagrams: [`docs/diagrams/airflow.drawio.svg`](docs/diagrams/airflow.drawio.svg) (the parallel run)
and [`docs/diagrams/kafka-export-v1.drawio.svg`](docs/diagrams/kafka-export-v1.drawio.svg) (one
table's export flow) — editable SVGs, so draw.io opens them directly.

### Result
None.

### Mistake
Phase 1 was too slow, too memory-hungry, and a retry costs a full run.

- `orders` alone took **~25 minutes** to drain and land as one Parquet object.
- **`order_items` (23M rows) did not fit in memory at all.** The whole partition sits in a Python
  list, then `pa.Table.from_pylist` builds the Arrow table on top — roughly double the footprint.
  That run was **OOM-killed**, and draining all 7 tables at once took `kafka` and `connect` down
  with it. The workaround was `kafka_to_s3_sequential` (`max_active_tasks=1`, one table at a time),
  which survives but is slower and still has no checkpointing.
- There is **one commit, at the very end**. If the task dies at 90%, nothing is committed and the
  next run re-reads that table **from the beginning** — a re-run is never cheaper than the first
  attempt.
- Nothing is uploaded until the whole partition is in memory, so polling Kafka and
  decode + Parquet write + upload never overlap.

### Lesson
Commit granularity has to match work granularity, and the buffer has to be bounded. One commit per
table means one failure costs the whole table, and one list per table means the biggest table can
never finish. Chunk the partition, upload each chunk, commit after each successful chunk — then a
failure costs one chunk instead of 25 minutes, and memory stays flat regardless of table size. That
is the entire reason for phase 2.

## Step 8 — Enhancement kafka_export.py

### What I did
Two-phase rebuild. First, cleanup because light tables (users, restaurant, reviews, food, menu) had already landed in S3 with the old single-partition, no-date-layout keys — I wiped the S3 prefix and the Kafka consumer group so nothing was double-counted or skipped.

**Phase 1 — Cleanup**
- `aws s3 rm s3://<BUCKET>/` (or wipe just the `load_date=` prefixes that had landed) to remove the old Parquet objects.
- Dropped the Debezium replication slot `debezium_zomato` and deleted the `zomato-source` connector via Kafka Connect REST (`DELETE /connectors/zomato-source`), plus the Kafka consumer group `airflow-kafka-to-s3` offsets, so a fresh snapshot starts from offset 0.
- Kept Postgres data intact (the source of truth) — only the CDC plumbing was reset.

**Phase 2 — Rebuild Kafka with per-topic partitioning**
Previously every Debezium topic had 1 partition (default). That meant `export_parallel` could not fan out within a table, and `order_items` (23M rows) OOM-killed the stack. Rebuilt with explicit per-topic partition counts set via `KAFKA_NUM_PARTITIONS` fallback + topic creation before connector registration:

| # | Table | Partitions | Rows | Reason |
|---|-------|-----------|------|--------|
| 1 | `order_items` | 12 | 22,998,179 | Biggest table, ~911 MiB. Highest fan-out so each partition chunk is small and memory stays flat.
| 2 | `orders` | 8 | 10,000,000 | Second largest, ~1.24 GiB. Fan-out keeps per-partition work bounded.
| 3 | `menu` | 4 | 1,179,936 | Mid-size; 4 partitions is enough to parallelize without over-splitting small chunks.
| 4 | `food` | 2 | 371,561 | Small enough that 2 partitions suffice.
| 5 | `reviews` | 2 | 300,000 | Small; 2 partitions for a little parallelism.
| 6 | `restaurant` | 1 | 148,541 | Tiny — more partitions than rows is wasteful.
| 7 | `users` | 1 | 100,000 | Tiny — single partition.

**Rationale:** partition count scales roughly with row count / size, capped so no partition gets so big it OOMs, and tiny tables aren't over-partitioned (a partition with 100k rows across 12 partitions = ~8k rows each, more overhead than gain). Total = 30 partitions across 7 tables. Connector's `table.include.list` stays the same; topics are pre-created with these counts before `connect-init` registers `zomato-source`.

### Commands
```sh
# cleanup S3 (adjust bucket)
aws s3 rm s3://<BUCKET>/ --recursive

# drop connector + slot so Debezium re-snapshots from scratch
curl -X DELETE http://localhost:8083/connectors/zomato-source
harlequin -a postgres "postgresql://admin:admin@localhost:5432/postgres"
# > SELECT pg_drop_replication_slot('debezium_zomato');

# restart stacks so new partition counts take effect
cd docker/kafka-debezium && docker compose --env-file ../../.env up -d --force-recreate
```

### New kafka_export.py approach
Replaced the old "accumulate whole partition in one list, upload once, commit at the end" function (the simple `export_partition` currently active at the bottom of the file) with a bounded chunked streamer:

- One `KafkaConsumer` per partition, `assign()` to a single partition, `enable_auto_commit=False`.
- Reader loop polls up to `CHUNK_MAX_RECORDS` records / `CHUNK_MAX_BYTES` bytes, accumulates a chunk, then flushes.
- **Each chunk is uploaded to S3 immediately** with key `<table>/load_date=<ds>/<table>-p<partition>-o<start>-<end>.parquet` (start/end are Kafka offsets, so a chunk is exactly one offset range).
- **Commit happens after each successful chunk upload** (`consumer.commit({tp: OffsetAndMetadata(end_offset+1)})`), never at the very end.
- On failure, only the current chunk is lost — the next run resumes from the last committed offset, never re-reads the whole table.
- Memory stays bounded: a chunk is at most `CHUNK_MAX_BYTES` (~32 MB) plus `CHUNK_MAX_RECORDS` (~50k) rows, regardless of table size.
- `export_parallel` keeps `ProcessPoolExecutor` fan-out across partitions, capped at `min(partitions, 2)` so local machines don't OOM.

The old single-shot function is removed; the commented reader-thread/uploader-thread version is also replaced by this simpler sync chunk+commit loop (same safety, less moving parts).

### Result
- S3 prefix clean; only the new partitioned, date-foldered Parquet objects remain.
- Kafka topics recreated with per-table partition counts; Debezium connector re-registered and RUNNING, fresh snapshot streaming from offset 0.
- `kafka_export.export_partition` now uploads+commits per chunk — `order_items` no longer needs to fit in RAM, and a failed run costs one chunk, not 25 minutes.

### Mistake
None.

### Lesson
Clean the sink and the consumer together when you change the export layout; otherwise old keys and committed offsets silently corrupt the new pipeline.

## Step 9 — Decimal decode before S3 landing + dict conversion moved into upload_to_s3

### What I did
Debezium serializes Postgres `NUMERIC`/`DECIMAL` columns as base64 by default (e.g. `12.50` -> unscaled `1250` -> bytes `0x04 0xE2` -> base64 `"BOI="`). If those raw base64 strings land in S3 as Parquet, downstream tools like Athena and Iceberg see opaque strings instead of real decimals. So I decode them **before** the Parquet is written to S3, not after the fact.

Two changes to `kafka_export.py`:

**1) Decode before landing to S3.** `upload_to_s3` now calls `decode_numerics(table, rows)` on the chunk right after converting raw bytes to dicts, turning base64-encoded numerics into proper `Decimal` values before `pa.Table.from_pylist` writes the Parquet buffer. The Parquet objects in S3 carry real `DECIMAL` types, so Athena/ Iceberg / any other consumer can query them directly without decoding.

**2) Dict conversion moved into `upload_to_s3`.** Previously the reader worker could convert raw Kafka bytes to dicts before enqueuing chunks. Now the reader worker passes raw bytes (`rec.value`) straight through, and `upload_to_s3` owns the `json.loads(r)` conversion. The reader stays dumb — it only polls, accumulates by record/byte limits, and hands off raw chunks. All decode/encode logic lives in the upload path.

Updated `upload_to_s3` flow:
```
raw bytes (rec.value)
  -> upload_to_s3
  -> json.loads(r)        # dict conversion (moved here from reader)
  -> decode_numerics(...) # base64 numeric -> Decimal (decode before S3)
  -> pa.Table.from_pylist # Parquet with real DECIMAL types
  -> put_object to S3
```

### Commands
No new commands — code change only in `docker/airflow/dags/kafka_export.py`. Restart Airflow to pick it up:
```sh
cd docker/airflow
docker compose --env-file ../../.env up -d --force-recreate  # or just touch the DAG file
```

### Result
- S3 Parquet objects contain real `DECIMAL` columns, not base64 strings — Athena and Iceberg can read them natively.
- Reader worker is decoupled from decode logic; it only moves raw bytes. Upload path owns parsing and decoding.
- Chunked upload + per-chunk commit from Step 8 still intact — failure still costs one chunk, not a whole table.

### Mistake
Originally left base64-encoded numerics in the landed Parquet, assuming downstream would decode. That breaks Athena/Iceberg and forces every consumer to re-implement Debezium's base64 numeric rule.

### Fix
Decode in `upload_to_s3` before `put_object`, so the S3 artifact is self-describing and ready for any downstream engine.

### Lesson
Encode/decode belongs at the sink boundary, not scattered across reader workers. Land data in the shape the widest set of consumers (Athena, Iceberg, Snowflake) expect.

## Step 10 — Snowflake + AWS IAM role for S3 read, COPY INTO landing with dedup columns

### What I did
Set up Snowflake as the warehouse target and wired an AWS IAM role so Snowflake can read the Parquet objects in S3 and `COPY INTO` the landing tables.

**Snowflake (`snowflake/`):**
- `001_setup.sql` — `ZOMATO_WAREHOUSE` (XSMALL, auto-suspend), `ZOMATO` database, schemas `LANDING / RAW / STAGING / MARTS / SNAPSHOTS / AI`, role `DBT_ROLE` with grants on warehouse, database, schemas, and future objects.
- `002_s3_integration.sql` — `ZOMATO_S3_INTEGRATION` (`TYPE=EXTERNAL_STAGE`, `STORAGE_PROVIDER=S3`, `STORAGE_AWS_ROLE_ARN='arn:aws:iam::678835429553:role/SnowflakeS3Role'`, `STORAGE_ALLOWED_LOCATIONS=('s3://zomato-pipeline-s3/')`), grant `USAGE` to `DBT_ROLE`. `DESC INTEGRATION` returns `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` for the AWS trust policy.
- `003_create_landing_table.sql` — one `ZOMATO.LANDING.<table>` per source table. Each has an `idx NUMBER IDENTITY` surrogate (auto-filled, never supplied by COPY), the source columns, plus the CDC dedup columns: `__op STRING` (Debezium op: c/u/d), `__source_ts_ms NUMBER` (source commit timestamp), `_load_date TIMESTAMP` (when loaded), `_filename STRING` (source Parquet file via `METADATA$FILENAME`).
- `004_create_stage.sql` — `ZOMATO.LANDING.S3_CDC_STAGE` (`URL='s3://zomato-pipeline-s3/'`, `STORAGE_INTEGRATION=ZOMATO_S3_INTEGRATION`, `FILE_FORMAT=(TYPE=PARQUET)`).

**AWS IAM role (`SnowflakeS3Role`):**
- Trust policy lets Snowflake (`STORAGE_AWS_IAM_USER_ARN`) assume the role, conditioned on `STORAGE_AWS_EXTERNAL_ID`.
- Permissions: `s3:GetObject`, `s3:ListBucket` on `arn:aws:s3:::zomato-pipeline-s3/` and `.../*`.

**COPY INTO landing (`docker/airflow/dags/sql/LANDING.<table>.sql`):**
- Maps `$1:<col>` from Parquet into typed columns, casts `__op`/`__source_ts_ms`, sets `_load_date = CURRENT_TIMESTAMP()`, `_filename = METADATA$FILENAME`.
- Partition pruning on `load_date={{ ds }}/` so each run only copies that day's objects.
- Append-only into landing — no upsert here; `__op`, `__source_ts_ms`, `_load_date`, `_filename` are kept so the raw layer can dedup and apply deletes.

### Commands
```sh
# Snowflake (run as ACCOUNTADMIN in Snowflake UI or SnowCLI)
snowsql -f snowflake/001_setup.sql
snowsql -f snowflake/002_s3_integration.sql
snowsql -f snowflake/003_create_landing_table.sql
snowsql -f snowflake/004_create_stage.sql

# AWS: create SnowflakeS3Role with trust policy using STORAGE_AWS_IAM_USER_ARN
# + STORAGE_AWS_EXTERNAL_ID from DESC INTEGRATION output
```

### Result
- Snowflake can read S3 Parquet via the storage integration.
- Landing tables exist for all 7 tables with CDC + dedup columns.
- Airflow `merge_<table>_to_snowflake` tasks run `COPY INTO` per `ds` partition.

### Mistake
First attempt relied on downstream consumers to decode base64 numerics and handle Debezium `__op` deletes themselves — every consumer had to re-implement the same logic.

### Fix
Store `__op`, `__source_ts_ms`, `_load_date`, `_filename` in landing at COPY time so the raw layer (dbt) owns dedup/delete logic once, not per-consumer.

### Lesson
Land data as close to the source shape as possible, but add the CDC/dedup columns at the sink so every downstream model can rely on them.

## Step 11 — Raw layer by dbt (incremental merge, no need manual upsert)

### What I did
Built the dbt `raw` layer (`zomato-dbt/zomato/models/raw/*.sql`) so it consumes `ZOMATO.LANDING` and produces `ZOMATO.RAW.<table>` using dbt's native `incremental` + `merge` strategy. No custom upsert/merge SQL — dbt handles CDC apply automatically.

**dbt_project.yml:**
```yaml
models:
  zomato:
    raw:
      +schema: RAW
      +materialized: incremental
      +incremental_strategy: merge
      +on_schema_change: append_new_columns
```

**Per-table model pattern (`models/raw/orders.sql`):**
- `materialized='incremental'`, `unique_key='<pk>'` (e.g. `order_id`, `review_id`, `user_id`, composite where needed).
- `incremental_strategy='merge'` — dbt upserts by `unique_key`; new keys insert, existing keys update.
- `on_schema_change='append_new_columns'` — safe for Debezium schema drift.
- CTE `source` reads `{{ source('landing', '<table>') }}`, filtered incrementally by `_load_date > coalesce(max(updated_at), '1970-01-01')` so only new landing rows are scanned.
- CTE `latest` picks the winning row per PK: `qualify row_number() over (partition by <pk> order by _load_date desc, idx desc) = 1` — latest CDC event wins.
- `coalesce(__op = 'd', false) as is_deleted` — soft-delete flag from Debezium op.
- Final select projects clean columns + `updated_at` + `is_deleted`.

**Why no manual upsert logic:**
Debezium CDC produces multiple events per PK (insert, then updates, possibly a delete). Landing is append-only with all events. dbt incremental merge on `unique_key` collapses them into one current row per PK in RAW. The `latest` CTE's `row_number()` ordering by `_load_date desc` ensures the most recent CDC event wins, and `__op='d'` marks deletes. So the raw layer is always eventually consistent with the source — no handwritten MERGE/UPSERT needed.

### Commands
```sh
cd zomato-dbt/zomato
# Run raw layer only
dbt run --select raw --project-dir . --profiles-dir .
# Test raw layer
dbt test --select raw --project-dir . --profiles-dir .
```

### Result
- `ZOMATO.RAW.<table>` for all 7 tables, incrementally maintained.
- Each RAW table holds one current row per PK, with `is_deleted` for CDC deletes.
- Airflow DAG: `dbt_build_raw` (`dbt run --select raw`) then `dbt_raw_test`.
- Staging, marts, enrichment, and embeddings all build on top of RAW.

### Mistake
Tried to apply CDC upserts in landing with Snowflake MERGE statements — complex, per-table SQL that duplicated what dbt already does.

### Fix
Landing stays append-only `COPY INTO` (dumb sink). All dedup/delete logic moves to dbt raw models using incremental merge — one pattern, all tables.

### Lesson
Push CDC resolution into the transformation layer (dbt merge), not the load layer (COPY). Keep landing dumb and append-only; let dbt's incremental merge handle idempotency.

---

## Step 11.1 - dbt tests: Landing + Raw (reference)

**Landing tests (`models/_sources.yml` — `source('landing', ...)`):**
Generic `not_null` tests on primary-key columns for all 7 source tables:
- `landing.restaurant.id` — not_null
- `landing.reviews.review_id` — not_null
- `landing.users.user_id` — not_null
- `landing.food.f_id` — not_null
- `landing.menu.menu_id` — not_null
- `landing.menu.f_id` — not_null
- `landing.orders.order_id` — not_null
- `landing.order_items.order_item_id` — not_null

These run via `dbt test --select 'source:*'` (Airflow task `dbt_landing_test`) to prove every landed batch still carries its PK before raw merge.

**Raw tests (`models/raw/_raw.yml` — per raw model) + custom SQL (`tests/`):**

| Table | Column | Tests |
|-------|--------|-------|
| `raw.food` | `f_id` | unique, not_null
| | `is_deleted` | not_null, accepted_values [true,false]
| | `updated_at` | not_null
| `raw.menu` | `menu_id` | not_null
| | `f_id` | not_null, relationships → `raw.food.f_id`
| | `r_id` | not_null, relationships → `raw.restaurant.id` (severity: warn)
| | `price` | not_null
| | `is_deleted` | not_null, accepted_values [true,false]
| | `updated_at` | not_null
| `raw.restaurant` | `id` | unique, not_null
| | `city` | not_null
| | `is_deleted` | not_null, accepted_values [true,false]
| | `updated_at` | not_null
| `raw.users` | `user_id` | unique, not_null
| | `name` | not_null
| | `email` | not_null
| | `is_deleted` | not_null, accepted_values [true,false]
| | `updated_at` | not_null
| `raw.order_items` | `order_item_id` | unique, not_null
| | `order_id` | not_null, relationships → `raw.orders.order_id`
| | `r_id` | not_null, relationships → `raw.restaurant.id`
| | `f_id` | not_null, relationships → `raw.food.f_id`
| | `price`, `quantity`, `line_amount` | not_null each
| | `is_deleted` | not_null, accepted_values [true,false]
| | `updated_at` | not_null
| `raw.orders` | `order_id` | unique, not_null
| | `order_timestamp`, `order_date`, `items_count`, `sales_qty`, `sales_amount`, `payment_method`, `order_status` | not_null each
| | `user_id` | not_null, relationships → `raw.users.user_id`
| | `r_id` | not_null, relationships → `raw.restaurant.id`
| | `is_deleted` | not_null, accepted_values [true,false]
| | `updated_at` | not_null
| `raw.reviews` | `review_id` | unique, not_null
| | `order_id` | not_null, relationships → `raw.orders.order_id`
| | `user_id` | not_null, relationships → `raw.users.user_id`
| | `restaurant_id` | not_null, relationships → `raw.restaurant.id`
| | `rating` | not_null, accepted_values [1,2,3,4,5]
| | `is_deleted` | not_null, accepted_values [true,false]
| | `updated_at` | not_null

**Custom SQL tests (`tests/`):**
- `tests/raw_menu_unique_key.sql` — `menu_id + f_id` must be unique (composite PK, group-by having count(*) > 1).

All raw tests run via `dbt test --select raw` (Airflow task `dbt_raw_test`) after `dbt_build_raw`.

## Step 12 — Staging build/test by dbt

### What I did
Built the dbt `staging` layer (`zomato-dbt/zomato/models/staging/stg_*.sql`) that reads from `ZOMATO.RAW.<table>` and produces cleaned, conformed views in `ZOMATO.STAGING`.

**dbt_project.yml:**
```yaml
models:
  zomato:
    staging:
      +schema: STAGING
      +materialized: view
```

**Per staging model pattern (`models/staging/stg_orders.sql` example):**
- `materialized='view'` — staging is always rebuilt from current RAW, no incremental state needed.
- Filters out CDC-deleted rows: `where is_deleted = false`.
- Casts/conforms types: `order_timestamp` → `TIMESTAMP`, epoch days → `DATE`, string columns trimmed.
- Renames columns to conformed names (e.g. `user_id` → `customer_id`, `r_id` → `restaurant_id`).
- Derives flags: `is_delivered = (order_status='Delivered')`.
- Reviews staging keeps only rows with a non-empty comment (feeds LLM enrichment + embeddings).

**Staging tests (`models/staging/_staging.yml`):**
Primary-key `unique` + `not_null`, `not_null` on all conformed columns, `accepted_values` on domains:
- `stg_orders.currency` ∈ ['INR']
- `stg_orders.payment_method` ∈ ['UPI','Card','Wallet','COD','NetBanking']
- `stg_orders.order_status` ∈ ['Delivered','Cancelled','Refunded']
- `stg_orders.is_delivered` ∈ [true,false]
- `stg_orders.customer_rating` ∈ [1,2,3,4,5] when present (nullable)
- `stg_restaurant.rating` range 1–5 (custom SQL test `tests/stg_restaurant_rating_range.sql`)
- `stg_users.gender` ∈ ['Male','Female']
- `stg_users.marital_status` ∈ ['Single','Married','Prefer not to say']
- `stg_food.veg_or_non_veg` ∈ ['Veg','Non-Veg','Unknown']
- Relationship (foreign-key) tests: `stg_order_items.order_id → stg_orders.order_id`, `stg_reviews.order_id → stg_orders.order_id`, etc. (some `severity: warn` where source data is known-incomplete).

**Custom SQL test:**
- `tests/stg_restaurant_rating_range.sql` — restaurant `rating` must be between 1 and 5 when present.

### Commands
```sh
cd zomato-dbt/zomato
dbt build --select staging --project-dir . --profiles-dir .   # run + test staging
dbt test --select staging --project-dir . --profiles-dir .   # tests only
```

### Result
- `ZOMATO.STAGING.<table>` views for all 7 tables, deleted rows excluded, types conformed.
- Staging tests catch bad domain values, missing PKs, and broken relationships before marts.
- Airflow DAG: `dbt_build_staging` (`dbt run --select staging`) → `dbt_staging_test`.
- Staging feeds marts, `stg_review_enrichment` (AI), `fct_reviews`, and `mart_restaurant_sentiment`.

### Mistake
Ran staging tests before filtering out `is_deleted = true` rows, so soft-deleted CDC records failed `not_null` / relationship tests against RAW.

### Fix
Add `where is_deleted = false` to every staging model so deleted CDC events never reach staging.

### Lesson
CDC deletes must be dropped at the first consumer of RAW (staging), not patched downstream in marts.

## Step 13 — Marts build/test by dbt

### What I did
Built the dbt `marts` layer (`zomato-dbt/zomato/models/marts/*.sql`) that reads from `ZOMATO.STAGING` + `ZOMATO.AI.REVIEW_ENRICHED` and produces star-schema tables in `ZOMATO.MARTS`.

**dbt_project.yml:**
```yaml
models:
  zomato:
    marts:
      +schema: MARTS
      +materialized: table
```

**Marts models:**
- Dimensions: `dim_customer`, `dim_restaurant`, `dim_food`, `dim_menu`, `dim_date`
- Facts: `fct_orders`, `fct_order_items`, `fct_reviews`
- Marts: `mart_daily_sales`, `mart_restaurant_performance`, `mart_customer_activity`, `mart_food_popularity`, `mart_city_daily_sales`, `mart_restaurant_sentiment`

**Key dependency:** `fct_reviews` joins `stg_review_enrichment` (from `source('ai', 'review_enriched')`), so `mart_restaurant_sentiment` needs LLM labels to exist before it builds. That is why marts run **after** `enrich_reviews` (Step 14), not right after staging.

**Marts tests (`models/marts/_marts.yml`):**
Primary-key `unique` + `not_null`, relationship tests to dims (some `severity: warn` where source data is incomplete), `accepted_values` on domains (e.g. `fct_reviews.rating` ∈ [1..5], `fct_reviews.is_enriched` ∈ [true,false], `sentiment_label` ∈ [positive,negative,neutral]).

### Commands
```sh
cd zomato-dbt/zomato
dbt build --select marts --project-dir . --profiles-dir .   # run + test marts
dbt test --select marts --project-dir . --profiles-dir .    # tests only
```

### Result
- `ZOMATO.MARTS.<table>` for all dims/facts/marts.
- `mart_restaurant_sentiment` populated once `enrich_reviews` has run.
- Airflow DAG: `dbt_build_marts` (`dbt run --select marts`) → `dbt_marts_test`.

### Mistake
Built marts before enrichment, so `fct_reviews` had null sentiment/topic and `mart_restaurant_sentiment` showed zero enriched reviews.

### Fix
Gate `dbt_build_marts` after `enrich_reviews` (Step 14) so AI labels exist first.

### Lesson
Marts consume AI-enriched labels — enrichment must precede marts in the DAG.

## Step 14 — Add enrich_reviews.py between staging and marts (marts depend on enriched labels)

### What I did
Wired `enrich_reviews.py` as an Airflow task that runs after `dbt_staging_test` and before `dbt_build_marts`, because `mart_restaurant_sentiment` (and `fct_reviews`) consume `source('ai', 'review_enriched')` -> `stg_review_enrichment` -> `fct_reviews`.

- Task `enrich_reviews` calls `enrich_reviews.main()`: reads `ZOMATO.STAGING.STG_REVIEWS`, classifies each comment with an LLM (sentiment label/score, topic, key issue), MERGE-serts into `ZOMATO.AI.REVIEW_ENRICHED` keyed by `REVIEW_ID`.
- DAG order: `... >> dbt_staging_test >> [enrich_reviews, embedding_reviews]` then `enrich_reviews >> dbt_build_marts >> dbt_marts_test`.
- `enrich_reviews` runs standalone: `python docker/airflow/dags/enrich_reviews.py`; knobs via `ENRICH_*` env vars (`ENRICH_WORKERS`, `ENRICH_BATCH_SIZE`, `ENRICH_BASE_URL`, `ENRICH_MODELS`).

### Commands
```sh
python docker/airflow/dags/enrich_reviews.py
```

### Result
- `ZOMATO.AI.REVIEW_ENRICHED` populated with per-review LLM labels.
- `dbt_build_marts` can now build `mart_restaurant_sentiment` from enriched reviews.

### Mistake
Ran marts before enrichment, so `fct_reviews` had null sentiment/topic and `mart_restaurant_sentiment` showed zero enriched reviews.

### Fix
Place `enrich_reviews` after staging (so comments exist) and before marts (so sentiment labels exist), gated by `dbt_staging_test`.

### Lesson
Enrichment is a prerequisite for marts that use AI labels — order the DAG: staging -> enrich -> marts.

## Step 15 — Drop OpenAI / Gemini, switch enrichment to local Ollama

### What I did
Removed `OPENAI_API_KEY` and `GEMINI_API_KEY` usage (pay-as-you-go / free-tier rate limits made bulk enrichment of 300k reviews uneconomic). Investigated local LLM options and switched to Ollama running on the host.

- Commented out OpenAI (`gpt-5.5`, `api.openai.com/v1`) and Gemini (`gemini-3.5-flash-lite`, `generativelanguage.googleapis.com`) blocks in `enrich_reviews.py`.
- Active model: `qwen2.5:3b` (small, fast, good enough for sentiment/topic classification), reachable at `ENRICH_BASE_URL` (default `http://host.docker.internal:11434`) so the Airflow container can reach the host's Ollama.
- Ollama must listen on `0.0.0.0` (`OLLAMA_HOST=0.0.0.0`) for the container to reach it at `host.docker.internal:11434`.
- Model pulled once on the host: `ollama pull qwen2.5:3b`.

### Commands
```sh
# on the host
OLLAMA_HOST=0.0.0.0 ollama serve &
ollama pull qwen2.5:3b

# run enrichment standalone
enrich_reviews.py  # via ENRICH_BASE_URL=http://host.docker.internal:11434 ENRICH_MODELS=qwen2.5:3b
```

### Result
- No OpenAI/Gemini API keys needed; enrichment runs fully local, zero per-call cost.
- `qwen2.5:3b` returns JSON matching `SCHEMA` (sentiment_label, sentiment_score, topic, key_issue).

### Mistake
Started with paid cloud models; hit rate limits and cost concerns before enriching a meaningful fraction of reviews.

### Fix
Remove cloud keys, use local Ollama with a small model (`qwen2.5:3b`).

### Lesson
For bulk LLM enrichment of CDC review streams, a local Ollama model is cheaper and unlimited; only pay cloud models for cases the local model can't handle.

## Step 16 — Add embedding task, sink to Snowflake ZOMATO.AI.REVIEW_EMBEDDINGS

### What I did
Added `embedding_reviews.py` as a parallel Airflow task (runs after staging alongside enrichment) that embeds review comments and sinks vectors into Snowflake.

- `embedding_reviews.main()`: reads `ZOMATO.STAGING.STG_REVIEWS`, left-joins `ZOMATO.AI.REVIEW_EMBEDDINGS` to skip already-embedded reviews, embeds each comment with Ollama `nomic-embed-text` (768 dims), MERGE-serts into `ZOMATO.AI.REVIEW_EMBEDDINGS` keyed by `REVIEW_ID`.
- Target table (`REVIEW_EMBEDDINGS`): `REVIEW_ID`, `ORDER_ID`, `RESTAURANT_ID`, `RATING`, `COMMENT`, `ROW_HASH`, `EMBEDDING VECTOR(FLOAT, 768)`, `EMBEDDING_MODEL`, `EMBEDDED_AT`.
- Knobs via `EMBED_*` env vars (`EMBED_BASE_URL`, `EMBED_MODELS`, `EMBED_DIM`); default model `nomic-embed-text` (768 dims, `ollama pull nomic-embed-text`).
- DAG task `embedding_reviews` calls `embedding_reviews.main()`; runs standalone with `python docker/airflow/dags/embedding_reviews.py`.

### Commands
```sh
ollama pull nomic-embed-text
python docker/airflow/dags/embedding_reviews.py
```

### Result
- `ZOMATO.AI.REVIEW_EMBEDDINGS` holds 768-dim vectors per review, merge-serted on `REVIEW_ID`.
- Embedding and enrichment run independently after staging.

### Mistake
Initially considered OpenAI `text-embedding-3-small` (1536 dims) — does not match the `VECTOR(FLOAT, 768)` column.

### Fix
Use `nomic-embed-text` (768 dims) to match `REVIEW_EMBEDDINGS.EMBEDDING`.

### Lesson
Embedding dimension must match the target vector column exactly; local Ollama `nomic-embed-text` gives 768 dims for free.

## Step 17 — Plan: Snowflake for DA/DE analytics; Postgres copy served to application via API

### What I did
Split downstream consumption of `ZOMATO.AI.REVIEW_EMBEDDINGS` by consumer:

**Snowflake — DA/DE team (analytics):**
- Snowflake is the source of truth for embeddings (`ZOMATO.AI.REVIEW_EMBEDDINGS`, 768-dim vectors).
- Data analysts / data engineers run ad-hoc similarity queries, dashboards, and reporting directly against Snowflake.

**Postgres copy — application serving layer (API call):**
- Application does not query Snowflake directly (latency, cost, credentials).
- `copy_embeddings_to_pg.main()` reads `ZOMATO.AI.REVIEW_EMBEDDINGS` from Snowflake and writes into the pipeline Postgres (`zomato-source`, Postgres 17 + pgvector).
- Application API reads from Postgres: receives a query, embeds it with `nomic-embed-text`, runs cosine similarity search against the Postgres copy via HNSW index, returns review matches.
- Airflow task `copy_embeddings_to_postgres` (after `embedding_reviews`), then `reindex_embeddings_hnsw` builds the HNSW index on the Postgres copy for fast similarity search.

**Chat feature on UI (application path):**
- User asks a question; UI/API embeds the query with `nomic-embed-text`.
- Cosine similarity search against the Postgres copy of `REVIEW_EMBEDDINGS.EMBEDDING`.
- LLM (local Ollama) answers from retrieved reviews (RAG).

### Commands
```sh
# copy Snowflake embeddings -> Postgres (Airflow task or standalone)
python docker/airflow/dags/copy_embeddings_to_pg.py

# build HNSW index on Postgres copy
python docker/airflow/dags/reindex_embeddings_hnsw.py
```

### Result
- Snowflake holds the canonical embeddings for DA/DE analytics.
- Postgres holds a read-optimized copy served to the application via API.
- Path cleared for chat UI / application backed by vector similarity search on Postgres.

### Mistake
Originally had the API read embeddings directly from Snowflake — too slow and expensive for per-request application traffic.

### Fix
Copy embeddings Snowflake -> Postgres; the application API queries the local Postgres copy (pgvector + HNSW), not Snowflake.

### Lesson
Separate analytics (Snowflake) from application serving (Postgres via API). DA/DE use Snowflake; the app API uses the Postgres copy.

## Step 18 — Upgrade Postgres 16 -> 17 for pg_vector (no data lost)

### What I did
The Postgres copy of embeddings needs `pgvector` for HNSW similarity search, but Postgres 16 (`zomato-source`) lacks `pg_vector` support in the installed image. Upgraded to Postgres 17 without losing data.

- Dumped the pipeline Postgres (schema + data) before the upgrade.
- Rebuilt `docker/postgres` stack on Postgres 17 image, installed/enabled `pgvector` extension.
- Restored the dump into the Postgres 17 container.
- Verified row counts match pre-upgrade (no data lost).
- `CREATE EXTENSION IF NOT EXISTS vector;` then `CREATE INDEX ... USING hnsw (embedding vector_cosine_ops)` on the Postgres copy of `REVIEW_EMBEDDINGS`.

### Commands
```sh
# pre-upgrade dump
pg_dump -h localhost -p 5432 -U <USER> <DB> > backup.sql

# switch docker/postgres to postgres:17, recreate stack
cd docker/postgres && docker compose --env-file ../../.env up -d --force-recreate

# restore
psql -h localhost -p 5432 -U <USER> <DB> < backup.sql

# enable pgvector
psql -c "CREATE EXTENSION IF NOT EXISTS vector;"
```

### Result
- Postgres 17 running with `pgvector`; all pre-upgrade data intact.
- HNSW index on review embeddings for cosine-similarity chat/search.

### Mistake
Started with Postgres 16, discovered `pg_vector` missing only when trying to create the vector index.

### Fix
Upgrade stack to Postgres 17 + pgvector; restore from dump.

### Lesson
Check extension availability before choosing the database version; pgvector needs Postgres 17 here.

## Step 19 — Enhancement: hash_row skip-unchanged + reindex stale vectors

### What I did
Added `hash_row` to avoid re-enriching / re-embedding reviews whose comment hasn't changed, plus a reindex task to clean up stale vectors when a comment is updated.

- `hash_row = HASH(comment)` computed on the staging comment.
- Enrichment (`enrich_reviews.py`) and embedding (`embedding_reviews.py`) both select only reviews where `target.REVIEW_ID IS NULL OR target.ROW_HASH != HASH(source.COMMENT)`. Identical comments cost one LLM / embedding call; changed comments are re-processed.
- Reindex task (`reindex_embeddings_hnsw`, after `copy_embeddings_to_postgres`): deletes Postgres vector rows whose `REVIEW_ID` no longer exists or whose hash changed, then rebuilds the HNSW index so stale vectors don't corrupt similarity search.
- DAG order: `embedding >> copy_embeddings_to_postgres >> reindex_embeddings_hnsw`.
- Runs standalone: `python docker/airflow/dags/embedding_reviews.py` (embedding) and `python docker/airflow/dags/reindex_embeddings_hnsw.py` (reindex).

### Commands
```sh
python docker/airflow/dags/enrich_reviews.py   # skips unchanged via ROW_HASH
python docker/airflow/dags/embedding_reviews.py # skips unchanged via ROW_HASH
python docker/airflow/dags/reindex_embeddings_hnsw.py  # purge stale vectors + rebuild HNSW
```

### Result
- Unchanged comments are skipped — re-runs are cheap and idempotent.
- When a comment updates, the old vector is removed and a new one embedded; HNSW index stays clean.

### Mistake
Re-embedded every review on every run, wasting LLM calls and leaving stale vectors in the index after comment updates.

### Fix
`ROW_HASH` gate before re-enrich/re-embed; reindex task purges stale vectors and rebuilds HNSW.

### Lesson
Hash the input, compare before calling the model, and reindex after mutation — keeps vector search correct and LLM/embedding cost minimal.

## Step 20 — Enrich/embed too slow: open 4 Ollama ports + multi-threaded API calls

### What I did
Single-threaded Ollama calls (`qwen2.5:3b` for enrichment, `nomic-embed-text` for embedding) were the bottleneck — one HTTP request at a time against `host.docker.internal:11434`, so 300k reviews took far too long.

Enhancement:
- Open **4 Ollama ports** on the host (`11434`, `11435`, `11436`, `11437`), each backed by the same Ollama instance listening on `0.0.0.0` (`OLLAMA_HOST=0.0.0.0`).
- `ENRICH_BASE_URL` / `EMBED_BASE_URL` now accept a comma-separated list of URLs.
- Add a **multi-threaded caller**: a `ThreadPoolExecutor` (default 4 workers) round-robins comments across the 4 Ollama endpoints, so up to 4 LLM/embedding requests fly in parallel.
- Each worker retries with exponential backoff on its assigned URL; failures are isolated to one thread, not the whole batch.

### Commands
```sh
# on the host, Ollama listening on all interfaces
OLLAMA_HOST=0.0.0.0 ollama serve &
ollama pull qwen2.5:3b
ollama pull nomic-embed-text

# run with 4 parallel endpoints
enrich_reviews.py   # ENRICH_BASE_URL=http://host.docker.internal:11434,:11435,:11436,:11437
embedding_reviews.py  # EMBED_BASE_URL likewise
```

### Result
- ~4x faster enrichment/embedding throughput (bounded by Ollama's concurrent request handling).
- No data change — same `ROW_HASH` skip-unchanged logic, just more parallel API calls.

### Mistake
Ran enrichment/embedding serially against one Ollama port; CPU sat idle while one request blocked the pipeline.

### Fix
Fan out across 4 Ollama ports with a thread pool.

### Lesson
Local LLM inference is IO-bound on HTTP; parallelize the caller, not just the model.

## Step 21 — Snowflake trial ended: new plan keeps everything in Postgres (see NEWPLAN.md)

### What I did
The Snowflake free trial ended, so the Snowflake-dependent path (`kafka_to_s3`, `LANDING.*.sql`, `zomato-dbt/`, `snowflake/`) can no longer run. New plan (full details in `NEWPLAN.md`): keep enrichment + embedding entirely in the local pipeline Postgres (`zomato-source`, Postgres 17 + pgvector).

**New DAG flow (`docker/airflow/dags/ai_enrich_embed_pg.py`, manual, `schedule=None`):**
- Import reviews **directly into Postgres** (`public.reviews`) instead of Kafka → S3 → Snowflake → dbt.
- `enrich_reviews_pg` → `embedding_reviews_pg` → `reindex_embeddings_hnsw`.
- Old Snowflake DAG (`pipeline.py`, `pipeline_sequential.py`, `enrich_reviews.py`, `embedding_reviews.py`, `copy_embeddings_to_pg.py`) is **kept untouched but not scheduled** — it can be revived if Snowflake returns.

**New cache table to skip re-enrich/re-embed of the same comment:**
- `public.review_enrichment_cache` — one row per distinct comment (`comment_hash = md5(comment)` PK), holds sentiment/topic/key_issue.
- `public.review_embeddings` — one row per review with `row_hash = md5(comment)`; skip when `row_hash` matches.
- Because `data/reviews.csv` has **300,000 reviews but only 294 distinct comment texts**, enrichment/embedding runs ~294 model calls instead of 300k — a massive API-call reduction.
- Fan-out: after computing labels/vectors for a distinct comment once, `INSERT ... SELECT` applies them to every review sharing that `md5(comment)`.

**New files (`docker/airflow/dags/`):**
- `utils/pg.py` — shared psycopg2 connection from `POSTGRES_*` env vars.
- `enrich_reviews_pg.py` — classifies distinct comments with Ollama, writes `review_enrichment_cache` + `review_enriched`.
- `embedding_reviews_pg.py` — embeds distinct comments (`nomic-embed-text`, 768 dims), writes `review_embeddings`.
- `ai_enrich_embed_pg.py` — the new manual DAG.
- `data/ddl.sql` — appended `review_enriched`, `review_enrichment_cache`, label indexes, and `reviews_comment_md5_idx` on `md5(comment)`.

### Commands
```sh
# standalone runs
python docker/airflow/dags/enrich_reviews_pg.py
python docker/airflow/dags/embedding_reviews_pg.py

# or via the new manual DAG: ai_enrich_embed_pg
```

### Result
- No Snowflake dependency for enrichment/embedding.
- ~294 model calls per full backfill instead of 300k (distinct-comment cache).
- Vectors stored in Postgres with pgvector; HNSW index rebuilt by `reindex_embeddings_hnsw`.

### Mistake
Designed the first version around Snowflake COPY + dbt, which broke when the trial ended and made every review a separate API call.

### Fix
Move enrichment/embedding to Postgres native; add `review_enrichment_cache` + `row_hash` guard to call each model once per distinct comment.

### Lesson
Cache by content hash when the same text repeats across many rows; it turns 300k API calls into ~294.

## Step 22 — <to be defined>

### What I did
Placeholder for the next step. Not defined yet.

### Result
None.

### Mistake
None.

### Fix
None.

### Lesson
None.

<!-- next step goes here -->
