"""
Gold layer aggregator — computes business metrics from silver data.
Data Lineage: MinIO/silver -> batch/gold_aggregator.py -> MinIO/gold (Parquet)
"""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from batch.spark_session import get_spark_session
from configs.logging_config import get_logger
from configs.spark_config import spark_config

logger = get_logger(__name__, "gold_aggregator.log")


class GoldAggregator:
    """
    Computes gold-layer business metrics from silver data.

    Produces:
    - daily_revenue: Revenue aggregated by day and product category
    - top_products: Best-selling products by revenue and order count
    - user_activity: Per-user engagement and spend metrics
    - category_performance: Category-level KPIs
    """

    def __init__(self, spark: SparkSession | None = None) -> None:
        self.spark = spark or get_spark_session(app_name="EcommerceGoldAggregator")
        self.silver = spark_config.silver_path
        self.gold = spark_config.gold_path

    def _read_silver(self, entity: str, process_date: date) -> DataFrame:
        path = f"{self.silver}/{entity}/process_date={process_date}"
        logger.info("Reading silver: %s", path)
        try:
            return self.spark.read.parquet(path)
        except Exception as exc:
            logger.warning("Silver data not found: %s | %s", path, exc)
            return self.spark.createDataFrame([], schema=None)

    def compute_daily_revenue(self, process_date: date) -> None:
        """
        Aggregate daily revenue by category.
        Output columns: order_date, category, total_revenue, total_orders, avg_order_value
        """
        orders = self._read_silver("orders", process_date)
        products = self._read_silver("products", process_date)
        if orders.rdd.isEmpty():
            logger.warning("No orders for daily_revenue on %s", process_date)
            return

        joined = orders.join(
            products.select("product_id", "category"),
            on="product_id",
            how="left",
        )

        agg = (
            joined.withColumn("order_date", F.to_date("order_time"))
            .filter(F.col("status") != "cancelled")
            .groupBy("order_date", "category")
            .agg(
                F.round(F.sum("total_amount"), 2).alias("total_revenue"),
                F.count("order_id").alias("total_orders"),
                F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
                F.sum("quantity").alias("total_units_sold"),
            )
            .withColumn("process_date", F.lit(str(process_date)))
        )

        out = f"{self.gold}/daily_revenue/process_date={process_date}"
        logger.info("Writing daily_revenue: %s rows=%d", out, agg.count())
        agg.write.mode("overwrite").parquet(out)

    def compute_top_products(self, process_date: date) -> None:
        """
        Compute product performance metrics.
        Output: product rankings by revenue, orders, and units sold.
        """
        orders = self._read_silver("orders", process_date)
        products = self._read_silver("products", process_date)
        if orders.rdd.isEmpty():
            return

        joined = orders.join(
            products.select("product_id", "name", "category", "price"),
            on="product_id",
            how="left",
        )

        agg = (
            joined.filter(F.col("status") != "cancelled")
            .groupBy("product_id", "name", "category", "price")
            .agg(
                F.round(F.sum("total_amount"), 2).alias("total_revenue"),
                F.count("order_id").alias("total_orders"),
                F.sum("quantity").alias("total_units_sold"),
                F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
            )
            .withColumn(
                "revenue_rank",
                F.rank().over(
                    F.window_spec("category").orderBy(F.col("total_revenue").desc())
                    if False
                    else __import__("pyspark.sql.window", fromlist=["Window"])
                    .Window.partitionBy("category")
                    .orderBy(F.col("total_revenue").desc())
                ),
            )
            .withColumn("process_date", F.lit(str(process_date)))
        )

        out = f"{self.gold}/top_products/process_date={process_date}"
        logger.info("Writing top_products: %s", out)
        agg.write.mode("overwrite").parquet(out)

    def compute_user_activity(self, process_date: date) -> None:
        """
        Compute per-user activity and spend metrics.
        Output: user-level aggregated engagement.
        """
        events = self._read_silver("events", process_date)
        orders = self._read_silver("orders", process_date)
        users = self._read_silver("users", process_date)

        if events.rdd.isEmpty() and orders.rdd.isEmpty():
            return

        event_agg = (
            events.groupBy("user_id")
            .agg(
                F.count("event_id").alias("total_events"),
                F.countDistinct("session_id").alias("total_sessions"),
                F.countDistinct(F.to_date("timestamp")).alias("active_days"),
                F.max("timestamp").alias("last_seen"),
            )
        ) if not events.rdd.isEmpty() else None

        order_agg = (
            orders.filter(F.col("status") != "cancelled")
            .groupBy("user_id")
            .agg(
                F.count("order_id").alias("total_orders"),
                F.round(F.sum("total_amount"), 2).alias("total_spend"),
                F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
                F.max("order_time").alias("last_order_time"),
            )
        ) if not orders.rdd.isEmpty() else None

        if event_agg and order_agg:
            combined = event_agg.join(order_agg, on="user_id", how="full")
        elif event_agg:
            combined = event_agg
        else:
            combined = order_agg

        if not users.rdd.isEmpty():
            combined = combined.join(
                users.select("user_id", "name", "email", "location"),
                on="user_id",
                how="left",
            )

        combined = combined.withColumn("process_date", F.lit(str(process_date)))
        out = f"{self.gold}/user_activity/process_date={process_date}"
        logger.info("Writing user_activity: %s", out)
        combined.write.mode("overwrite").parquet(out)

    def compute_sales_trends(self, process_date: date) -> None:
        """
        Compute hourly and daily sales trends for time-series analysis.
        """
        orders = self._read_silver("orders", process_date)
        if orders.rdd.isEmpty():
            return

        hourly = (
            orders.filter(F.col("status") != "cancelled")
            .withColumn("order_hour", F.date_trunc("hour", F.col("order_time")))
            .groupBy("order_hour")
            .agg(
                F.round(F.sum("total_amount"), 2).alias("hourly_revenue"),
                F.count("order_id").alias("hourly_orders"),
            )
            .withColumn("process_date", F.lit(str(process_date)))
        )

        out = f"{self.gold}/sales_trends/process_date={process_date}"
        logger.info("Writing sales_trends: %s", out)
        hourly.write.mode("overwrite").parquet(out)

    def run(self, process_date: date | None = None) -> None:
        from datetime import date as d
        target = process_date or (d.today() - timedelta(days=1))
        logger.info("Starting gold aggregation for date: %s", target)
        self.compute_daily_revenue(target)
        self.compute_top_products(target)
        self.compute_user_activity(target)
        self.compute_sales_trends(target)
        logger.info("Gold aggregation complete for %s", target)


if __name__ == "__main__":
    import sys
    from datetime import date as d
    process_date = d.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    GoldAggregator().run(process_date)
