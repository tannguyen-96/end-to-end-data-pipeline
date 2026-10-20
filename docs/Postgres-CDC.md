# PostgreSQL Change Data Capture (CDC)

## What is CDC?

Change Data Capture (CDC) is a design pattern that identifies and captures changes made to data in a database (inserts, updates, deletes) and delivers them as a stream of change events to downstream systems. Instead of polling tables or running batch diffs, CDC observes the database's transaction log to capture changes in near real-time with minimal impact on the source system.

## Why PostgreSQL for CDC?

PostgreSQL is well-suited for CDC because it provides:

- **Write-Ahead Log (WAL)**: All changes are first written to the WAL for durability and crash recovery. This log is the source of truth for what changed.
- **Logical Decoding**: PostgreSQL can decode WAL entries into a logical, human-readable format (e.g., `INSERT`/`UPDATE`/`DELETE` with column values) rather than physical byte-level changes.
- **Replication Slots**: Ensure consumers don't miss changes and can resume from a specific position in the WAL.
- **Publication/Subscription Model**: Native logical replication allows selective tables and fine-grained control over what is replicated.

## How CDC Works in PostgreSQL

1. **Transactions occur**: Applications perform `INSERT`, `UPDATE`, or `DELETE` operations on tables.
2. **WAL is written**: PostgreSQL records these changes to the Write-Ahead Log before committing.
3. **Logical decoding extracts changes**: A logical decoder (e.g., `wal2json`, `pgoutput`, `decoderbufs`) reads WAL entries and translates them into logical change events.
4. **Events are published**: Tools like Debezium capture these events and publish them to message brokers (e.g., Kafka) or directly to consumers.
5. **Downstream systems consume**: Consumers process events for replication, analytics, caching, or event-driven architectures.

## Common PostgreSQL CDC Approaches

| Approach | How it works | Pros | Cons |
|---|---|---|---|
| **WAL-based (Logical Decoding)** | Read WAL via replication slots using a decoder plugin. | Minimal overhead, captures all changes, no schema changes required. | Requires `wal_level=logical`, replication slot management. |
| **Debezium + Kafka Connect** | Uses logical decoding to stream changes to Kafka topics. | Scalable, fault-tolerant, rich ecosystem, easy integration. | More infrastructure (Kafka, Connect). |
| **Trigger-based** | PostgreSQL triggers write changes to a change table. | Simple, works on older versions, easy to customize. | Higher overhead, can miss direct SQL outside app, harder to scale. |
| **pglogical/Logical Replication** | Native logical replication between Postgres instances. | Built-in, reliable, good for replication. | Less flexible for event streaming to non-Postgres systems. |

## CDC in This Project

In this repository, PostgreSQL CDC is used as follows:

1. **Source**: Application PostgreSQL (`zomato-source`) acts as the source of truth. In a real scenario, Debezium would read the WAL directly from the live application database.
2. **CDC**: Debezium (via Kafka Connect) reads WAL using logical decoding and publishes to Kafka topics `pg.public.*`.
3. **Streaming**: Airflow orchestrates `kafka_to_s3` to consume topics, chunk events, and write them as Parquet to S3 (`<table>/load_date=<ds>/`).
4. **Loading**: Events are loaded into Snowflake via `COPY INTO` with deduplication using `__source_ts_ms` and `__op`.
5. **Transformation**: dbt models transform the data through raw → staging → marts layers.

This is a **WAL-based CDC pipeline** using Debezium + Kafka, streaming changes from Postgres to S3 and ultimately to Snowflake for analytics.

## Key Considerations

- **WAL retention**: Ensure replication slots are properly managed to avoid WAL buildup.
- **Schema changes**: Handle DDL changes as they occur in the source.
- **Exactly-once semantics**: Use idempotent consumers and track offsets (as implemented in `kafka_export.py`).
- **Network/availability**: Keep Kafka Connect and replication slots healthy to avoid missing changes.
