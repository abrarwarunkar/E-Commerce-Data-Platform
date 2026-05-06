"""
Unit tests for stream validation logic.
"""

import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from streaming.stream_validator import (
    validate_events_df,
    validate_orders_df,
    enforce_schema_types,
)


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .appName("TestValidator")
        .master("local[1]")
        .getOrCreate()
    )


def make_events_df(spark, rows):
    schema = StructType([
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
    return spark.createDataFrame(rows, schema)


def make_orders_df(spark, rows):
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("order_time", StringType(), True),
    ])
    return spark.createDataFrame(rows, schema)


class TestEventValidation:
    def test_valid_event_passes(self, spark):
        rows = [("e1", "u1", "page_view", None, "s1", "/home", None, "desktop", "2024-01-01")]
        df = validate_events_df(make_events_df(spark, rows))
        assert df.count() == 1

    def test_null_event_id_filtered(self, spark):
        rows = [(None, "u1", "page_view", None, "s1", None, None, "mobile", "2024-01-01")]
        df = validate_events_df(make_events_df(spark, rows))
        assert df.count() == 0

    def test_null_user_id_filtered(self, spark):
        rows = [("e1", None, "page_view", None, "s1", None, None, "mobile", "2024-01-01")]
        df = validate_events_df(make_events_df(spark, rows))
        assert df.count() == 0

    def test_invalid_event_type_filtered(self, spark):
        rows = [("e1", "u1", "unknown_type", None, "s1", None, None, "desktop", "2024-01-01")]
        df = validate_events_df(make_events_df(spark, rows))
        assert df.count() == 0

    def test_invalid_device_type_filtered(self, spark):
        rows = [("e1", "u1", "page_view", None, "s1", None, None, "smartwatch", "2024-01-01")]
        df = validate_events_df(make_events_df(spark, rows))
        assert df.count() == 0

    def test_multiple_rows_partial_valid(self, spark):
        rows = [
            ("e1", "u1", "page_view", None, "s1", None, None, "desktop", "2024-01-01"),
            ("e2", None, "page_view", None, "s2", None, None, "mobile", "2024-01-01"),
            ("e3", "u3", "bad_type", None, "s3", None, None, "tablet", "2024-01-01"),
        ]
        df = validate_events_df(make_events_df(spark, rows))
        assert df.count() == 1


class TestOrderValidation:
    def test_valid_order_passes(self, spark):
        rows = [("o1", "u1", "p1", 2, 50.0, 100.0, "confirmed", "2024-01-01")]
        df = validate_orders_df(make_orders_df(spark, rows))
        assert df.count() == 1

    def test_null_order_id_filtered(self, spark):
        rows = [(None, "u1", "p1", 2, 50.0, 100.0, "confirmed", "2024-01-01")]
        df = validate_orders_df(make_orders_df(spark, rows))
        assert df.count() == 0

    def test_zero_quantity_filtered(self, spark):
        rows = [("o1", "u1", "p1", 0, 50.0, 0.0, "pending", "2024-01-01")]
        df = validate_orders_df(make_orders_df(spark, rows))
        assert df.count() == 0

    def test_invalid_status_filtered(self, spark):
        rows = [("o1", "u1", "p1", 1, 10.0, 10.0, "unknown_status", "2024-01-01")]
        df = validate_orders_df(make_orders_df(spark, rows))
        assert df.count() == 0
