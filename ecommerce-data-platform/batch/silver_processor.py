"""
Silver layer batch processor — cleans and deduplicates bronze data.
Data Lineage: MinIO/bronze -> batch/silver_processor.py -> MinIO/silver (Parquet)
"""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from batch.spark_session import get_spark_session
from configs.logging_config import get_logger
from configs.spark_config import spark_config

logger = get_logger(__name__, "silver_processor.log")


class SilverProcessor:
    """
    Transforms bronze (raw) data into silver (cleaned, deduplicated) layer.
    Steps: read incremental date partition -> deduplicate -> cast types -> write silver.
    """

    def __init__(self, spark: SparkSession | None = None) -> None:
        self.spark = spark or get_spark_session(app_name="EcommerceSilverProcessor")
        self.bronze = spark_config.bronze_path
        self.silver = spark_config.silver_path

    def _read_bronze_partition(self, entity: str, process_date: date) -> DataFrame:
        path = (
            f"{self.bronze}/{entity}"
            f"/year={process_date.year}/month={process_date.month}/day={process_date.day}"
        )
        logger.info("Reading bronze partition: %s", path)
        try:
            return self.spark.read.parquet(path)
        except Exception as exc:
            logger.warning("Bronze partition not found: %s | %s", path, exc)
            return self.spark.createDataFrame([], schema=None)

    def _dedup(self, df: DataFrame, pk: str, ts_col: str) -> DataFrame:
        """Keep latest record per primary key."""
        window = Window.partitionBy(pk).orderBy(F.col(ts_col).desc())
        return (
            df.withColumn("_row_num", F.row_number().over(window))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

    def _drop_kafka_cols(self, df: DataFrame) -> DataFrame:
        kafka_cols = ["_kafka_key", "_kafka_topic", "_kafka_partition", "_kafka_offset"]
        return df.drop(*[c for c in kafka_cols if c in df.columns])

    def process_events(self, process_date: date) -> None:
        logger.info("Processing silver events for %s", process_date)
        df = self._read_bronze_partition("events", process_date)
        if df.rdd.isEmpty():
            return
        cleaned = (
            df.filter(F.col("event_id").isNotNull() & F.col("user_id").isNotNull())
            .withColumn("timestamp", F.to_timestamp("timestamp"))
            .withColumn("device_type", F.lower(F.col("device_type")))
            .withColumn("event_type", F.lower(F.col("event_type")))
            .transform(self._drop_kafka_cols)
        )
        deduped = self._dedup(cleaned, "event_id", "timestamp")
        deduped = deduped.withColumn("process_date", F.lit(str(process_date)))
        out_path = f"{self.silver}/events/process_date={process_date}"
        logger.info("Writing silver events: %s rows=%d", out_path, deduped.count())
        deduped.write.mode("overwrite").parquet(out_path)

    def process_orders(self, process_date: date) -> None:
        logger.info("Processing silver orders for %s", process_date)
        df = self._read_bronze_partition("orders", process_date)
        if df.rdd.isEmpty():
            return
        cleaned = (
            df.filter(
                F.col("order_id").isNotNull()
                & F.col("user_id").isNotNull()
                & (F.col("total_amount") > 0)
            )
            .withColumn("order_time", F.to_timestamp("order_time"))
            .withColumn("status", F.lower(F.col("status")))
            .withColumn("quantity", F.col("quantity").cast("integer"))
            .withColumn("unit_price", F.col("unit_price").cast("double"))
            .withColumn("total_amount", F.col("total_amount").cast("double"))
            .transform(self._drop_kafka_cols)
        )
        deduped = self._dedup(cleaned, "order_id", "order_time")
        deduped = deduped.withColumn("process_date", F.lit(str(process_date)))
        out_path = f"{self.silver}/orders/process_date={process_date}"
        logger.info("Writing silver orders: %s rows=%d", out_path, deduped.count())
        deduped.write.mode("overwrite").parquet(out_path)

    def process_users(self, process_date: date) -> None:
        logger.info("Processing silver users for %s", process_date)
        df = self._read_bronze_partition("users", process_date)
        if df.rdd.isEmpty():
            return
        cleaned = (
            df.filter(F.col("user_id").isNotNull() & F.col("email").isNotNull())
            .withColumn("created_at", F.to_timestamp("created_at"))
            .withColumn("email", F.lower(F.trim(F.col("email"))))
            .withColumn("name", F.trim(F.col("name")))
            .transform(self._drop_kafka_cols)
        )
        deduped = self._dedup(cleaned, "user_id", "created_at")
        deduped = deduped.withColumn("process_date", F.lit(str(process_date)))
        out_path = f"{self.silver}/users/process_date={process_date}"
        deduped.write.mode("overwrite").parquet(out_path)

    def process_products(self, process_date: date) -> None:
        logger.info("Processing silver products for %s", process_date)
        df = self._read_bronze_partition("products", process_date)
        if df.rdd.isEmpty():
            return
        cleaned = (
            df.filter(F.col("product_id").isNotNull() & (F.col("price") > 0))
            .withColumn("created_at", F.to_timestamp("created_at"))
            .withColumn("category", F.lower(F.col("category")))
            .withColumn("price", F.col("price").cast("double"))
            .transform(self._drop_kafka_cols)
        )
        deduped = self._dedup(cleaned, "product_id", "created_at")
        deduped = deduped.withColumn("process_date", F.lit(str(process_date)))
        out_path = f"{self.silver}/products/process_date={process_date}"
        deduped.write.mode("overwrite").parquet(out_path)

    def run(self, process_date: date | None = None) -> None:
        from datetime import date as d
        target = process_date or (d.today() - timedelta(days=1))
        logger.info("Starting silver processing for date: %s", target)
        self.process_events(target)
        self.process_orders(target)
        self.process_users(target)
        self.process_products(target)
        logger.info("Silver processing complete for %s", target)


if __name__ == "__main__":
    import sys
    from datetime import date as d
    process_date = d.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    SilverProcessor().run(process_date)
