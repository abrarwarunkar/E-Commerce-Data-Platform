"""
Spark configuration for the E-Commerce Data Platform.
Data Lineage: configs/spark_config.py -> batch/, streaming/
"""

import os
from dataclasses import dataclass, field
from typing import Dict, Any


@dataclass
class SparkConfig:
    """PySpark session and job configuration."""

    app_name: str = "EcommerceDataPlatform"
    master: str = field(
        default_factory=lambda: os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    )

    # MinIO / S3 settings
    minio_endpoint: str = field(
        default_factory=lambda: os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    )
    minio_access_key: str = field(
        default_factory=lambda: os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    )
    minio_secret_key: str = field(
        default_factory=lambda: os.getenv("MINIO_SECRET_KEY", "minioadmin")
    )
    minio_bucket: str = field(
        default_factory=lambda: os.getenv("MINIO_BUCKET", "ecommerce-data-lake")
    )

    # Data lake paths
    bronze_path: str = field(
        default_factory=lambda: f"s3a://{os.getenv('MINIO_BUCKET', 'ecommerce-data-lake')}/bronze"
    )
    silver_path: str = field(
        default_factory=lambda: f"s3a://{os.getenv('MINIO_BUCKET', 'ecommerce-data-lake')}/silver"
    )
    gold_path: str = field(
        default_factory=lambda: f"s3a://{os.getenv('MINIO_BUCKET', 'ecommerce-data-lake')}/gold"
    )

    # Kafka integration
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    )

    # Spark configs
    spark_conf: Dict[str, Any] = field(default_factory=lambda: {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.sql.shuffle.partitions": "8",
        "spark.streaming.stopGracefullyOnShutdown": "true",
        # MinIO / Hadoop S3A configs
        "spark.hadoop.fs.s3a.endpoint": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
        # Checkpointing
        "spark.sql.streaming.checkpointLocation": "/tmp/spark-checkpoints",
    })

    # Streaming settings
    trigger_interval: str = os.getenv("SPARK_TRIGGER_INTERVAL", "60 seconds")
    watermark_delay: str = os.getenv("SPARK_WATERMARK_DELAY", "10 minutes")


spark_config = SparkConfig()
