"""
Kafka configuration for the E-Commerce Data Platform.
Data Lineage: configs/kafka_config.py -> ingestion/, streaming/
"""

import os
from dataclasses import dataclass, field
from typing import Dict, Any


@dataclass
class KafkaConfig:
    """Kafka broker and topic configuration."""

    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    )
    schema_registry_url: str = field(
        default_factory=lambda: os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    )

    # Topics
    TOPIC_USER_EVENTS: str = "user-events"
    TOPIC_ORDERS: str = "orders"
    TOPIC_PRODUCTS: str = "products"
    TOPIC_USERS: str = "users"

    ALL_TOPICS: list = field(default_factory=lambda: [
        "user-events", "orders", "products", "users"
    ])

    # Producer settings
    producer_config: Dict[str, Any] = field(default_factory=lambda: {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "client.id": "ecommerce-producer",
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
        "linger.ms": 10,
        "batch.size": 16384,
        "compression.type": "snappy",
        "enable.idempotence": True,
    })

    # Consumer settings
    consumer_config: Dict[str, Any] = field(default_factory=lambda: {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "group.id": "ecommerce-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "session.timeout.ms": 30000,
        "max.poll.interval.ms": 300000,
    })

    # Topic settings
    num_partitions: int = int(os.getenv("KAFKA_NUM_PARTITIONS", "3"))
    replication_factor: int = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))


kafka_config = KafkaConfig()
