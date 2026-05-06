"""
Shared PySpark session factory with MinIO/S3A integration.
Data Lineage: batch/spark_session.py -> silver_processor.py, gold_aggregator.py, streaming/
"""

from __future__ import annotations

import os
from typing import Optional

from pyspark.sql import SparkSession

from configs.logging_config import get_logger
from configs.spark_config import spark_config

logger = get_logger(__name__, "spark_session.log")

_session: Optional[SparkSession] = None


def get_spark_session(
    app_name: str | None = None,
    local_mode: bool = False,
) -> SparkSession:
    """
    Create or retrieve a singleton SparkSession.

    Args:
        app_name: Override default application name
        local_mode: Use local[*] master instead of cluster master

    Returns:
        Configured SparkSession
    """
    global _session

    if _session and not _session.sparkContext._jsc.sc().isStopped():
        return _session

    name = app_name or spark_config.app_name
    master = "local[*]" if local_mode else spark_config.master

    logger.info("Creating SparkSession: app=%s master=%s", name, master)

    builder = SparkSession.builder.appName(name).master(master)

    # Apply all Spark/Hadoop configs
    for key, value in spark_config.spark_conf.items():
        builder = builder.config(key, value)

    # Additional packages for Kafka and S3
    builder = builder.config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        ]),
    )

    _session = builder.getOrCreate()
    _session.sparkContext.setLogLevel("WARN")

    logger.info(
        "SparkSession created | version=%s | master=%s",
        _session.version,
        _session.sparkContext.master,
    )
    return _session


def stop_spark_session() -> None:
    """Stop the active SparkSession."""
    global _session
    if _session:
        logger.info("Stopping SparkSession...")
        _session.stop()
        _session = None
