"""Settings shared by the producer, consumer and dashboard."""
import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "return-events"

HDFS_URL = "http://namenode:9870"       # WebHDFS
HDFS_USER = "hadoop"
RAW_DIR = "/data/returns/raw"           # the consumer stores Kafka events here
OUTPUT_DIR = "/data/returns/output"     # the MapReduce job writes weekly results here

THRESHOLD = 0.10                        # categories above a 10% return rate are flagged


def retry(action, what, attempts=60, delay=5):
    """Run action() until it succeeds. Containers start at different speeds, so the first tries can fail."""
    for attempt in range(1, attempts + 1):
        try:
            return action()
        except Exception as exc:
            print(f"Waiting for {what} ({attempt}/{attempts}): {exc}", flush=True)
            time.sleep(delay)
    raise RuntimeError(f"{what} is not reachable")


def ensure_topic():
    """Create the Kafka topic (3 partitions, keyed by category) if it does not exist yet."""
    admin = retry(lambda: KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP), "Kafka")
    try:
        admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=1)])
        print(f"Created Kafka topic '{TOPIC}'", flush=True)
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()
