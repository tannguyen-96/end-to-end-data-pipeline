from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
import base64
from decimal import Decimal
import json
import os
import queue
import threading
import time

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition

TABLES = ["menu", "orders", "users", "restaurant", "reviews", "food", "order_items"]

NUMERIC_COLUMNS: dict[str, dict[str, int]] = {
    "menu": {"price": 2},
    "orders": {
        "subtotal": 2,
        "discount": 2,
        "delivery_fee": 2,
        "gst": 2,
        "sales_amount": 2,
        "customer_rating": 1,
        "delivery_time_min": 1,
    },
    "restaurant": {"rating": 1},
    "order_items": {"price": 2, "line_amount": 2},
}

def _decode_numeric(value: str | None, scale: int) -> Decimal | None:
    """base64 (two's-complement big-endian, unscaled) -> Decimal scaled by 10**-scale."""
    if value is None or not isinstance(value, str):
        return value
    bytes = base64.b64decode(value)
    unscaled = int.from_bytes(bytes, byteorder="big", signed=True)
    return Decimal(unscaled).scaleb(-scale)


def decode_numerics(table: str, rows: list) -> list:
    """Rewrite Debezium base64 `numeric` columns to Decimal in-place (Parquet keeps precision)."""
    columns = NUMERIC_COLUMNS.get(table)
    if not columns:
        return rows
    for row in rows:
        for column, scale in columns.items():
            if column in row:
                row[column] = _decode_numeric(row[column], scale)
    return rows


TOPIC_PREFIX = "pg.public"
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "airflow-kafka-to-s3")

CHUNK_MAX_RECORDS = int(os.environ.get("CHUNK_MAX_RECORDS", "50_000"))
CHUNK_MAX_BYTES = int(os.environ.get("CHUNK_MAX_BYTES", "32_000_000"))
QUEUE_SIZE = int(os.environ.get("QUEUE_SIZE", "2"))
UPLOAD_WORKERS = int(os.environ.get("UPLOAD_WORKERS", "2"))
UPLOAD_MAX_RETRIES = int(os.environ.get("UPLOAD_MAX_RETRIES", "5"))
UPLOAD_BACKOFF_BASE_S = float(os.environ.get("UPLOAD_BACKOFF_BASE_S", "1"))
IDLE_TIMEOUT_MS = int(os.environ.get("IDLE_TIMEOUT_MS", "10_000"))


def _s3_key(table: str, partition: int, ds: str, start: int, end: int) -> str:
    # table/load_date=2026-01-01/table-p0-o0-50000.parquet
    return f"{table}/load_date={ds}/{table}-p{partition}-o{start}-{end}.parquet"


def upload_to_s3(table: str, partition: int, ds: str, start: int, end: int,
                 rows: list) -> None:
    """Serialize one chunk to Parquet and upload, retrying with exponential backoff."""
    if not rows:
        return
    # Sample encode value 12.50 -> unscaled 1250 -> bytes 0x04 0xE2 -> base64 "BOI="
    rows = decode_numerics(table, rows) # base64 numeric -> Decimal before Parquet
    buffer = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist(rows), buffer)
    body = buffer.getvalue().to_pybytes() # parquet format bytes

    bucket = os.environ["S3_BUCKET"]
    key = _s3_key(table, partition, ds, start, end)
    client = boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION")) # get s3 client
    attempt = 1
    while True:
        try:
            client.put_object(Bucket=bucket, Key=key, Body=body) # try upload to s3
            break
        except Exception as exc:  # if any failed
            if attempt >= UPLOAD_MAX_RETRIES:
                raise  # throw error after exceeding max retries
            delay = UPLOAD_BACKOFF_BASE_S * (2 ** (attempt - 1)) # backoff delay standard improve with backoff and jitter https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
            print(f"{table} p{partition}: upload retry {attempt}/{UPLOAD_MAX_RETRIES} "
                  f"in {delay:.0f}s after {exc!r}")
            time.sleep(delay)
            attempt += 1
    print(f"{table}: {len(rows)} records -> s3://{bucket}/{key}")


def export_partition(table: str, partition: int, ds: str) -> int:
    topic = f"{TOPIC_PREFIX}.{table}"
    tp = TopicPartition(topic, partition)
    print(f"Exporting partition: {tp}")
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",  # first run picks up the Debezium snapshot
        enable_auto_commit=False,      # set manual commit offset
        value_deserializer=lambda b: json.loads(b) if b else None,
    )
    try:
        consumer.assign([tp])
        end = consumer.end_offsets([tp])[tp]
        print(f"Partition end offset: {end}")

        chunks: queue.Queue = queue.Queue(maxsize=QUEUE_SIZE)
        results: dict[int, int] = {}        # chunk seq -> end offset (uploaded OK)
        first_failure: list = []            # [(seq, exc)] once a chunk gives up
        total: list[int] = [0]              # records uploaded this run
        lock = threading.Lock()

        def reader() -> None:
            """Poll the partition -> emit bounded chunks (put() blocks = backpressure)."""
            seq, rows, start, size, last = 0, [], None, 0, None

            # reset after put to the chunks
            def flush() -> None:
                nonlocal seq, rows, start, size
                if not rows:
                    return
                chunks.put((seq, start, last + 1, rows))
                total[0] += len(rows)
                seq += 1
                rows, start, size = [], None, 0

            try: # loop partition
                while consumer.position(tp) < end:
                    batch = consumer.poll(timeout_ms=IDLE_TIMEOUT_MS,
                                          max_records=CHUNK_MAX_RECORDS)
                    for records in batch.values():
                        for rec in records:
                            if start is None:
                                start = rec.offset # set start = first read offset
                            last = rec.offset # set each offset after as last (on first message start = last)
                            rows.append(rec.value)
                            size += rec.serialized_value_size
                            # if reach max message or max bytes, flush the chunk
                            if len(rows) >= CHUNK_MAX_RECORDS or size >= CHUNK_MAX_BYTES:
                                flush()
                flush() # flush the rest
            finally:
                for _ in range(UPLOAD_WORKERS):
                    chunks.put(None) # after each end offset put a None to chunks

        def uploader() -> None:
            while True: # start loop
                item = chunks.get() # get the next chunk (blocks if none available)
                if item is None:
                    return # break if receive None
                seq, start, end_off, rows = item # use to build s3 filename
                with lock:
                    already_failed = bool(first_failure) # True if any failed
                if not already_failed: # prevent further uploads if any chunk has already failed
                    try:
                        upload_to_s3(table, partition, ds, start, end_off, rows)
                        with lock:
                            results[seq] = end_off # save the end offset after upload chunk ex: { 0: 50000, 1: 100000 }
                    except Exception as exc:
                        with lock:
                            if not first_failure:
                                first_failure.append((seq, exc)) # record any failed

        workers = [threading.Thread(target=uploader, daemon=True)
                   for _ in range(UPLOAD_WORKERS)]
        for w in workers: # start 2 worker threads listening for chunks
            w.start()
        reader() # start reading and chunking the data
        for w in workers:
            w.join() # wait for all worker threads to finish

        if first_failure:
            seq, exc = first_failure[0]
            raise RuntimeError(f"{topic} p{partition}: chunk {seq} upload failed after "
                               f"{UPLOAD_MAX_RETRIES} retries: {exc!r}") from exc

        committed = 0
        seq = 0
        while seq in results: # loop and find latest committed offset
            committed = results[seq]
            seq += 1
        if committed:
            # kafka-python 3.x requires OffsetAndMetadata
            consumer.commit({tp: OffsetAndMetadata(committed)})
        print(f"{topic} p{partition}: {total[0]} records committed @ {committed}")
        return total[0]
    finally:
        consumer.close()


def export_parallel(table: str, ds: str) -> int:
    probe = KafkaConsumer(bootstrap_servers=BOOTSTRAP_SERVERS)
    partitions = sorted(probe.partitions_for_topic(f"{TOPIC_PREFIX}.{table}"))
    probe.close()

    # Min 2 workers prevent OOM local machine
    with ProcessPoolExecutor(max_workers=min(len(partitions), 2)) as pool:
        results = pool.map(export_partition,
                           [table] * len(partitions),
                           partitions,
                           [ds] * len(partitions))
        return sum(results)