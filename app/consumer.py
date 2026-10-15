"""Reads return events from Kafka and stores them in HDFS as JSON-lines files.

Events are written in batches (BATCH_SIZE events or FLUSH_SECONDS, whichever comes first), so HDFS gets
a few medium files instead of thousands of tiny ones. Kafka offsets are committed only after a batch is
safely in HDFS, so a crash or restart never loses events.
"""
import time
import uuid
from datetime import datetime

from hdfs import InsecureClient
from kafka import KafkaConsumer

from common import HDFS_URL, HDFS_USER, KAFKA_BOOTSTRAP, RAW_DIR, TOPIC, ensure_topic, retry

BATCH_SIZE = 20000
FLUSH_SECONDS = 10


def write_batch(hdfs, lines):
    name = f"events-{datetime.now():%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:6]}.jsonl"
    in_progress = f"{RAW_DIR}/_{name}"          # MapReduce ignores files starting with "_" ...
    # Pass bytes: given a str + encoding, the hdfs library streams it in tiny chunks (~100x slower).
    hdfs.write(in_progress, data=("\n".join(lines) + "\n").encode("utf-8"))
    hdfs.rename(in_progress, f"{RAW_DIR}/{name}")   # ... so the job never reads a half-written file
    print(f"Wrote {len(lines):,} events to {RAW_DIR}/{name}", flush=True)


def main():
    ensure_topic()
    hdfs = InsecureClient(HDFS_URL, user=HDFS_USER)
    retry(lambda: hdfs.makedirs(RAW_DIR), "HDFS")
    consumer = retry(lambda: KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="hdfs-writer",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    ), "Kafka")
    print(f"Consuming '{TOPIC}' -> hdfs://{RAW_DIR}", flush=True)

    buffer, first_event_at = [], 0.0
    while True:
        for records in consumer.poll(timeout_ms=1000, max_records=5000).values():
            if not buffer:
                first_event_at = time.time()
            buffer.extend(record.value.decode("utf-8") for record in records)

        if buffer and (len(buffer) >= BATCH_SIZE or time.time() - first_event_at >= FLUSH_SECONDS):
            write_batch(hdfs, buffer)
            consumer.commit()
            buffer = []


if __name__ == "__main__":
    main()
