"""
Kafka producer for all e-commerce topics.
Data Lineage: ingestion/kafka_producer.py -> Kafka topics -> streaming/spark_streaming.py
"""

import json
import os
import signal
import sys
import time
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from configs.kafka_config import kafka_config
from configs.logging_config import get_logger
from ingestion.data_generator import EcommerceDataGenerator
from ingestion.schemas import OrderSchema, ProductSchema, UserSchema

logger = get_logger(__name__, "kafka_producer.log")


class EcommerceKafkaProducer:
    """
    Production Kafka producer for e-commerce data.

    Features:
    - Idempotent delivery (exactly-once semantics)
    - Delivery callbacks for monitoring
    - Retry with exponential backoff
    - Graceful shutdown
    """

    def __init__(self) -> None:
        self._producer: Optional[Producer] = None
        self._running = False
        self._stats = {
            "messages_sent": 0,
            "messages_failed": 0,
            "bytes_sent": 0,
        }
        self._connect()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
    def _connect(self) -> None:
        """Establish connection to Kafka broker with retry."""
        logger.info(
            "Connecting to Kafka: %s", kafka_config.bootstrap_servers
        )
        self._producer = Producer(kafka_config.producer_config)
        logger.info("Kafka producer connected successfully.")

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """
        Callback invoked on message delivery success or failure.
        Used for monitoring and metrics.
        """
        if err:
            logger.error(
                "Delivery failed | topic=%s partition=%s offset=%s | error=%s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                err,
            )
            self._stats["messages_failed"] += 1
        else:
            self._stats["messages_sent"] += 1
            self._stats["bytes_sent"] += len(msg.value())

    def _serialize(self, record: Any) -> bytes:
        """Serialize a Pydantic model or dict to JSON bytes."""
        if hasattr(record, "model_dump"):
            data = record.model_dump(mode="json")
        elif isinstance(record, dict):
            data = record
        else:
            raise TypeError(f"Cannot serialize type: {type(record)}")
        return json.dumps(data, default=str).encode("utf-8")

    def publish(
        self,
        topic: str,
        key: str,
        value: Any,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Publish a message to a Kafka topic.

        Args:
            topic: Target Kafka topic
            key: Message key (used for partitioning)
            value: Message value (Pydantic model or dict)
            headers: Optional Kafka headers
        """
        serialized = self._serialize(value)
        kafka_headers = list((k, v.encode()) for k, v in (headers or {}).items())
        kafka_headers.append(("source", b"ecommerce-platform"))
        kafka_headers.append(
            ("ingestion_time", datetime.utcnow().isoformat().encode())
        )

        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=serialized,
            headers=kafka_headers,
            on_delivery=self._delivery_callback,
        )
        # Poll to trigger delivery callbacks
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> None:
        """Flush all pending messages."""
        remaining = self._producer.flush(timeout=timeout)
        if remaining:
            logger.warning("%d messages were not delivered after flush.", remaining)

    def close(self) -> None:
        """Graceful shutdown."""
        logger.info("Flushing producer before shutdown...")
        self.flush()
        logger.info(
            "Producer stats: sent=%d, failed=%d, bytes=%d",
            self._stats["messages_sent"],
            self._stats["messages_failed"],
            self._stats["bytes_sent"],
        )

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def ensure_topics_exist(topics: list[str]) -> None:
    """Create Kafka topics if they don't already exist."""
    admin = AdminClient({"bootstrap.servers": kafka_config.bootstrap_servers})
    existing = set(admin.list_topics(timeout=10).topics.keys())

    new_topics = [
        NewTopic(
            t,
            num_partitions=kafka_config.num_partitions,
            replication_factor=kafka_config.replication_factor,
        )
        for t in topics
        if t not in existing
    ]

    if new_topics:
        results = admin.create_topics(new_topics)
        for topic, future in results.items():
            try:
                future.result()
                logger.info("Created Kafka topic: %s", topic)
            except Exception as exc:
                logger.warning("Topic %s may already exist: %s", topic, exc)
    else:
        logger.info("All Kafka topics already exist.")


def run_producer(runtime_seconds: int = 0) -> None:
    """
    Main producer loop — generates and publishes all data types.

    Args:
        runtime_seconds: Run duration (0 = infinite)
    """
    gen = EcommerceDataGenerator(
        num_users=int(os.getenv("NUM_USERS", "1000")),
        num_products=int(os.getenv("NUM_PRODUCTS", "500")),
        events_per_second=float(os.getenv("EVENTS_PER_SECOND", "10")),
    )

    # Ensure topics exist
    ensure_topics_exist(kafka_config.ALL_TOPICS)

    with EcommerceKafkaProducer() as producer:
        # Bootstrap: publish all users and products first
        logger.info("Publishing %d users to Kafka...", len(gen.users))
        for user in gen.users:
            producer.publish(
                topic=kafka_config.TOPIC_USERS,
                key=user.user_id,
                value=user,
                headers={"entity": "user"},
            )

        logger.info("Publishing %d products to Kafka...", len(gen.products))
        for product in gen.products:
            producer.publish(
                topic=kafka_config.TOPIC_PRODUCTS,
                key=product.product_id,
                value=product,
                headers={"entity": "product"},
            )
        producer.flush()
        logger.info("Bootstrap data published. Starting event stream...")

        # Set up graceful shutdown
        _stop = False

        def _handle_signal(sig, frame):
            nonlocal _stop
            logger.info("Received signal %s — shutting down gracefully...", sig)
            _stop = True

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

        start = time.time()
        for record_type, record in gen.mixed_stream():
            if _stop:
                break
            if runtime_seconds and (time.time() - start) >= runtime_seconds:
                logger.info("Runtime limit reached (%ds), stopping.", runtime_seconds)
                break

            if record_type == "order":
                producer.publish(
                    topic=kafka_config.TOPIC_ORDERS,
                    key=record.order_id,
                    value=record,
                    headers={"entity": "order"},
                )
            else:
                producer.publish(
                    topic=kafka_config.TOPIC_USER_EVENTS,
                    key=record.event_id,
                    value=record,
                    headers={"entity": "event"},
                )


if __name__ == "__main__":
    runtime = int(os.getenv("GENERATOR_RUNTIME_SECONDS", "0"))
    run_producer(runtime_seconds=runtime)
