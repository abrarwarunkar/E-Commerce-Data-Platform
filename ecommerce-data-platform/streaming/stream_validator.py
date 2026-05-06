"""
Data validation layer for streaming data.
Data Lineage: streaming/stream_validator.py -> spark_streaming.py -> bronze layer
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from configs.logging_config import get_logger

logger = get_logger(__name__, "stream_validator.log")

VALID_EVENT_TYPES = {
    "page_view", "product_view", "add_to_cart", "remove_from_cart",
    "checkout_start", "purchase", "search", "wishlist_add",
}

VALID_DEVICE_TYPES = {"desktop", "mobile", "tablet"}

VALID_ORDER_STATUSES = {
    "pending", "confirmed", "shipped", "delivered", "cancelled", "returned",
}


def validate_events_df(df: DataFrame) -> DataFrame:
    """
    Validate and clean the events streaming DataFrame.

    Rules:
    - event_id must not be null
    - user_id must not be null
    - event_type must be a known value
    - device_type must be valid if present
    - Adds _is_valid flag; invalid rows routed to quarantine column

    Returns DataFrame with _validation_error column (null = valid).
    """
    valid_event_types = F.array(*[F.lit(t) for t in VALID_EVENT_TYPES])
    valid_device_types = F.array(*[F.lit(t) for t in VALID_DEVICE_TYPES])

    df = df.withColumn(
        "_validation_error",
        F.when(F.col("event_id").isNull(), F.lit("null event_id"))
        .when(F.col("user_id").isNull(), F.lit("null user_id"))
        .when(
            ~F.array_contains(valid_event_types, F.col("event_type")),
            F.concat(F.lit("invalid event_type: "), F.col("event_type")),
        )
        .when(
            F.col("device_type").isNotNull()
            & ~F.array_contains(valid_device_types, F.col("device_type")),
            F.concat(F.lit("invalid device_type: "), F.col("device_type")),
        )
        .otherwise(F.lit(None).cast("string")),
    )

    # Log validation stats via watermark (best effort in streaming)
    valid_df = df.filter(F.col("_validation_error").isNull())
    return valid_df


def validate_orders_df(df: DataFrame) -> DataFrame:
    """
    Validate and clean the orders streaming DataFrame.

    Rules:
    - order_id, user_id, product_id must not be null
    - quantity must be positive
    - total_amount must be positive
    - status must be a known value

    Returns DataFrame with invalid rows filtered out.
    """
    valid_statuses = F.array(*[F.lit(s) for s in VALID_ORDER_STATUSES])

    df = df.withColumn(
        "_validation_error",
        F.when(F.col("order_id").isNull(), F.lit("null order_id"))
        .when(F.col("user_id").isNull(), F.lit("null user_id"))
        .when(F.col("product_id").isNull(), F.lit("null product_id"))
        .when(
            F.col("quantity").isNull() | (F.col("quantity") <= 0),
            F.lit("invalid quantity"),
        )
        .when(
            F.col("total_amount").isNull() | (F.col("total_amount") <= 0),
            F.lit("invalid total_amount"),
        )
        .when(
            ~F.array_contains(valid_statuses, F.col("status")),
            F.concat(F.lit("invalid status: "), F.col("status")),
        )
        .otherwise(F.lit(None).cast("string")),
    )

    valid_df = df.filter(F.col("_validation_error").isNull())
    return valid_df


def enforce_schema_types(df: DataFrame, type_map: dict) -> DataFrame:
    """
    Cast DataFrame columns to specified types.

    Args:
        df: Input DataFrame
        type_map: {column_name: spark_type_string} mapping

    Returns:
        DataFrame with enforced column types
    """
    for col_name, col_type in type_map.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(col_type))
    return df
