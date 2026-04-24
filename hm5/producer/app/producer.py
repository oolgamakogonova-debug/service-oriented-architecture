import logging
import time
from confluent_kafka import Producer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
    StringSerializer,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from app.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, SCHEMA_REGISTRY_URL
from app.schemas import MovieEvent

logger = logging.getLogger(__name__)


def _event_to_dict(event: MovieEvent, ctx) -> dict:
    return {
        "event_id": str(event.event_id),
        "user_id": event.user_id,
        "movie_id": event.movie_id,
        "event_type": event.event_type.value,
        "timestamp": int(event.timestamp.timestamp() * 1000),
        "device_type": event.device_type.value,
        "session_id": event.session_id,
        "progress_seconds": event.progress_seconds,
    }


class KafkaProducer:
    def __init__(self):
        self._sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        self._avro_serializer = AvroSerializer(
            schema_registry_client=self._sr_client,
            schema_str=self._get_schema(),
            to_dict=_event_to_dict,
        )
        self._string_serializer = StringSerializer("utf_8")
        self._producer = Producer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "acks": "all",
                "retries": 5,
                "retry.backoff.ms": 200,
                "enable.idempotence": True,
                "max.in.flight.requests.per.connection": 5,
                "linger.ms": 10,
                "batch.size": 32768,
            }
        )
        logger.info("KafkaProducer initialized with servers: %s", KAFKA_BOOTSTRAP_SERVERS)

    def _get_schema(self) -> str:
        import json
        import os

        schema_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "avro",
            "movie_event.avsc",
        )
        # Fallback: fetch from Schema Registry
        try:
            with open(schema_path) as f:
                return f.read()
        except FileNotFoundError:
            schema = self._sr_client.get_latest_version("movie-events-value")
            return schema.schema.schema_str

    def _delivery_callback(self, err, msg):
        if err:
            logger.error("Delivery failed: %s", err)
        else:
            logger.info(
                "Published event to %s [partition=%d, offset=%d]",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    def send(self, event: MovieEvent) -> None:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self._producer.produce(
                    topic=KAFKA_TOPIC,
                    key=self._string_serializer(event.user_id),
                    value=self._avro_serializer(
                        event,
                        SerializationContext(KAFKA_TOPIC, MessageField.VALUE),
                    ),
                    on_delivery=self._delivery_callback,
                )
                self._producer.poll(0)
                logger.info(
                    "Sent event_id=%s event_type=%s timestamp=%s",
                    event.event_id,
                    event.event_type.value,
                    event.timestamp.isoformat(),
                )
                return
            except BufferError:
                logger.warning(
                    "Producer buffer full, retrying (%d/%d)...",
                    attempt + 1,
                    max_retries,
                )
                self._producer.poll(1)
                time.sleep(0.5 * (2**attempt))
            except Exception as e:
                logger.error("Failed to produce event: %s", e)
                if attempt == max_retries - 1:
                    raise
                time.sleep(0.5 * (2**attempt))

    def flush(self):
        self._producer.flush(timeout=10)