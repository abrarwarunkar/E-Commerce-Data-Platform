"""
Spark Structured Streaming consumer — reads from Kafka, validates, and writes to bronze layer.
Data Lineage: Kafka topics -> streaming/spark_streaming.py -> MinIO/bronze (Parquet)
"""

import signal
import sys
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from batch.spark_session import get_spark_session
from configs.kafka_config import kafka_config
from configs.logging_config import get_logger
from configs.spark_config import spark_config
from streaming.stream_validator import validate_events_df, validate_orders_df

logger = get_logger(__name__, "spark_streaming.log")


# ── Spark schemas for Kafka payloads ──────────────────────────────────────

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("timestamp", StringType(), True),
])

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("order_time", StringType(), True),
])

USER_SCHEMA = StructType([
    StructField("user_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("location", StringType(), True),
    StructField("created_at", StringType(), True),
])

PRODUCT_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("description", StringType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("created_at", StringType(), True),
])


def read_kafka_stream(spark: SparkSession, topic: str) -> DataFrame:
    """
    Create a streaming DataFrame from a Kafka topic.

    Args:
        spark: Active SparkSession
        topic: Kafka topic to subscribe to

    Returns:
        Raw streaming DataFrame with Kafka metadata columns
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_config.bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 10000)
        .load()
    )


def parse_kafka_stream(raw_df: DataFrame, schema: StructType) -> DataFrame:
    """
    Parse raw Kafka bytes into structured DataFrame.

    Args:
        raw_df: Raw Kafka streaming DataFrame
        schema: Target schema for the value payload

    Returns:
        Parsed DataFrame with ingestion metadata
    """
    return (
        raw_df.select(
            F.col("key").cast(StringType()).alias("_kafka_key"),
            F.from_json(F.col("value").cast(StringType()), schema).alias("data"),
            F.col("topic").alias("_kafka_topic"),
            F.col("partition").alias("_kafka_partition"),
            F.col("offset").alias("_kafka_offset"),
            F.col("timestamp").alias("_kafka_timestamp"),
        )
        .select(
            "data.*",
            "_kafka_key",
            "_kafka_topic",
            "_kafka_partition",
            "_kafka_offset",
            "_kafka_timestamp",
        )
        .withColumn("_ingestion_time", F.current_timestamp())
    )


def add_partition_columns(df: DataFrame, ts_col: str) -> DataFrame:
    """
    Add date-based partition columns for efficient data lake storage.

    Args:
        df: Input DataFrame
        ts_col: Name of the timestamp column to partition on

    Returns:
        DataFrame with year/month/day/hour partition columns
    """
    ts = F.to_timestamp(F.col(ts_col))
    return (
        df.withColumn(ts_col, ts)
        .withColumn("year", F.year(ts))
        .withColumn("month", F.month(ts))
        .withColumn("day", F.dayofmonth(ts))
        .withColumn("hour", F.hour(ts))
    )


def write_bronze_stream(
    df: DataFrame,
    entity: str,
    checkpoint_suffix: str = "",
) -> "pyspark.sql.streaming.StreamingQuery":
    """
    Write a streaming DataFrame to the bronze data lake layer.

    Args:
        df: Validated streaming DataFrame
        entity: Entity name (events, orders, users, products)
        checkpoint_suffix: Unique suffix for checkpoint path

    Returns:
        Active StreamingQuery
    """
    output_path = f"{spark_config.bronze_path}/{entity}"
    checkpoint_path = f"/tmp/spark-checkpoints/bronze/{entity}{checkpoint_suffix}"

    logger.info(
        "Writing bronze stream: entity=%s path=%s", entity, output_path
    )

    return (
        df.writeStream.format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("year", "month", "day", "hour")
        .trigger(processingTime=spark_config.trigger_interval)
        .outputMode("append")
        .start()
    )


def run_streaming_pipeline() -> None:
    """
    Main streaming pipeline entry point.

    Reads from all Kafka topics, validates, and writes to bronze layer.
    Handles graceful shutdown on SIGINT/SIGTERM.
    """
    spark = get_spark_session(app_name="EcommerceStreaming")
    queries = []

    try:
        # ── Events stream ─────────────────────────────────────────────────
        logger.info("Starting events streaming pipeline...")
        events_raw = read_kafka_stream(spark, kafka_config.TOPIC_USER_EVENTS)
        events_df = parse_kafka_stream(events_raw, EVENT_SCHEMA)
        events_valid = validate_events_df(events_df)
        events_partitioned = add_partition_columns(events_valid, "timestamp")
        q_events = write_bronze_stream(events_partitioned, "events")
        queries.append(q_events)

        # ── Orders stream ─────────────────────────────────────────────────
        logger.info("Starting orders streaming pipeline...")
        orders_raw = read_kafka_stream(spark, kafka_config.TOPIC_ORDERS)
        orders_df = parse_kafka_stream(orders_raw, ORDER_SCHEMA)
        orders_valid = validate_orders_df(orders_df)
        orders_partitioned = add_partition_columns(orders_valid, "order_time")
        q_orders = write_bronze_stream(orders_partitioned, "orders")
        queries.append(q_orders)

        # ── Users stream (upsert-style, smaller volume) ───────────────────
        logger.info("Starting users streaming pipeline...")
        users_raw = read_kafka_stream(spark, kafka_config.TOPIC_USERS)
        users_df = parse_kafka_stream(users_raw, USER_SCHEMA)
        users_partitioned = add_partition_columns(users_df, "created_at")
        q_users = write_bronze_stream(users_partitioned, "users")
        queries.append(q_users)

        # ── Products stream ───────────────────────────────────────────────
        logger.info("Starting products streaming pipeline...")
        products_raw = read_kafka_stream(spark, kafka_config.TOPIC_PRODUCTS)
        products_df = parse_kafka_stream(products_raw, PRODUCT_SCHEMA)
        products_partitioned = add_partition_columns(products_df, "created_at")
        q_products = write_bronze_stream(products_partitioned, "products")
        queries.append(q_products)

        logger.info(
            "All %d streaming queries started. Waiting for termination...",
            len(queries),
        )

        # Graceful shutdown
        def _handle_signal(sig, frame):
            logger.info("Signal %s received — stopping streaming queries...", sig)
            for q in queries:
                q.stop()

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

        # Wait for all queries to complete
        for q in queries:
            q.awaitTermination()

    except Exception as exc:
        logger.error("Streaming pipeline error: %s", exc, exc_info=True)
        for q in queries:
            try:
                q.stop()
            except Exception:
                pass
        sys.exit(1)
    finally:
        logger.info("Streaming pipeline shut down.")


if __name__ == "__main__":
    run_streaming_pipeline()
