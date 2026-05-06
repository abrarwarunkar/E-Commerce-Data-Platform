"""
PostgreSQL data warehouse loader — moves gold data into the star schema.
Data Lineage: MinIO/gold -> warehouse/postgres_loader.py -> PostgreSQL (star schema)
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import date, timedelta
from typing import Generator, List

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker
from tenacity import retry, stop_after_attempt, wait_exponential

from configs.logging_config import get_logger
from warehouse.models import Base, DimDate, DimProduct, DimUser, FactOrder, FactEvent

logger = get_logger(__name__, "postgres_loader.log")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://ecommerce:ecommerce_secret@postgres:5432/ecommerce_dw",
)


class PostgresLoader:
    """
    Loads transformed gold-layer data into the PostgreSQL data warehouse.

    Features:
    - Upsert semantics (ON CONFLICT DO UPDATE) for idempotency
    - Automatic date dimension population
    - Batch inserts for performance
    - Retry with exponential backoff
    """

    def __init__(self, database_url: str = DATABASE_URL) -> None:
        self.engine = self._create_engine(database_url)
        self.SessionLocal = sessionmaker(bind=self.engine, expire_on_commit=False)
        logger.info("PostgresLoader initialized: %s", database_url.split("@")[-1])

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    def _create_engine(self, url: str):
        engine = create_engine(
            url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection verified.")
        return engine

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def create_schema(self) -> None:
        """Create all tables if they don't exist."""
        logger.info("Creating warehouse schema...")
        Base.metadata.create_all(self.engine)
        logger.info("Schema created successfully.")

    def populate_date_dimension(self, start_date: date, end_date: date) -> None:
        """Populate the dim_date table for a date range."""
        logger.info("Populating dim_date from %s to %s", start_date, end_date)
        rows = []
        current = start_date
        while current <= end_date:
            dt = current
            rows.append({
                "date_sk": int(dt.strftime("%Y%m%d")),
                "full_date": dt,
                "year": dt.year,
                "quarter": (dt.month - 1) // 3 + 1,
                "month": dt.month,
                "month_name": dt.strftime("%B"),
                "week_of_year": dt.isocalendar()[1],
                "day_of_month": dt.day,
                "day_of_week": dt.weekday(),
                "day_name": dt.strftime("%A"),
                "is_weekend": 1 if dt.weekday() >= 5 else 0,
            })
            current += timedelta(days=1)

        with self.engine.begin() as conn:
            stmt = insert(DimDate).values(rows)
            stmt = stmt.on_conflict_do_nothing(index_elements=["date_sk"])
            conn.execute(stmt)
        logger.info("Inserted %d date dimension rows.", len(rows))

    def upsert_users(self, df: pd.DataFrame) -> int:
        """Upsert users into dim_users."""
        if df.empty:
            return 0
        rows = df[["user_id", "name", "email", "location", "created_at"]].to_dict("records")
        with self.engine.begin() as conn:
            stmt = insert(DimUser).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["user_id"],
                set_={"name": stmt.excluded.name, "email": stmt.excluded.email,
                      "location": stmt.excluded.location},
            )
            conn.execute(stmt)
        logger.info("Upserted %d users.", len(rows))
        return len(rows)

    def upsert_products(self, df: pd.DataFrame) -> int:
        """Upsert products into dim_products."""
        if df.empty:
            return 0
        rows = df[["product_id", "name", "category", "price", "description"]].to_dict("records")
        with self.engine.begin() as conn:
            stmt = insert(DimProduct).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["product_id"],
                set_={"name": stmt.excluded.name, "category": stmt.excluded.category,
                      "price": stmt.excluded.price},
            )
            conn.execute(stmt)
        logger.info("Upserted %d products.", len(rows))
        return len(rows)

    def load_fact_orders(self, df: pd.DataFrame) -> int:
        """Load orders into fact_orders with surrogate key lookups."""
        if df.empty:
            return 0

        with self.engine.connect() as conn:
            user_map = {
                r.user_id: r.user_sk
                for r in conn.execute(text("SELECT user_id, user_sk FROM dim_users"))
            }
            product_map = {
                r.product_id: r.product_sk
                for r in conn.execute(text("SELECT product_id, product_sk FROM dim_products"))
            }

        rows = []
        for _, row in df.iterrows():
            user_sk = user_map.get(row["user_id"])
            product_sk = product_map.get(row["product_id"])
            if not user_sk or not product_sk:
                continue
            order_time = pd.to_datetime(row["order_time"])
            date_sk = int(order_time.strftime("%Y%m%d"))
            rows.append({
                "order_id": row["order_id"],
                "user_sk": user_sk,
                "product_sk": product_sk,
                "date_sk": date_sk,
                "quantity": int(row["quantity"]),
                "unit_price": float(row["unit_price"]),
                "total_amount": float(row["total_amount"]),
                "status": row["status"],
                "order_time": order_time,
            })

        if rows:
            with self.engine.begin() as conn:
                stmt = insert(FactOrder).values(rows)
                stmt = stmt.on_conflict_do_nothing(index_elements=["order_id"])
                conn.execute(stmt)
        logger.info("Loaded %d fact_orders rows.", len(rows))
        return len(rows)

    def run(self, silver_path: str, process_date: date) -> None:
        """
        Full load pipeline for a given processing date.

        Args:
            silver_path: Base path to silver Parquet files
            process_date: Date partition to load
        """
        import pyarrow.parquet as pq

        self.create_schema()
        self.populate_date_dimension(
            date(process_date.year, 1, 1),
            date(process_date.year, 12, 31),
        )

        def read_parquet(entity: str) -> pd.DataFrame:
            path = f"{silver_path}/{entity}/process_date={process_date}"
            try:
                return pq.read_table(path).to_pandas()
            except Exception as exc:
                logger.warning("Could not read %s: %s", path, exc)
                return pd.DataFrame()

        self.upsert_users(read_parquet("users"))
        self.upsert_products(read_parquet("products"))
        self.load_fact_orders(read_parquet("orders"))
        logger.info("Warehouse load complete for %s", process_date)


if __name__ == "__main__":
    import sys
    from datetime import date as d
    process_date = d.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else d.today() - timedelta(days=1)
    silver_path = os.getenv("SILVER_PATH", "/data/silver")
    PostgresLoader().run(silver_path, process_date)
