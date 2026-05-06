"""
SQLAlchemy models for the PostgreSQL data warehouse (star schema).
Data Lineage: warehouse/models.py -> schema_creator.py, postgres_loader.py
"""

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


# ── Dimension Tables ───────────────────────────────────────────────────────

class DimUser(Base):
    """User dimension table — SCD Type 1 (overwrite on change)."""
    __tablename__ = "dim_users"

    user_sk = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(String(36), nullable=False, unique=True, index=True)
    name = Column(String(255), nullable=False)
    email = Column(String(255), nullable=False)
    location = Column(String(255))
    created_at = Column(DateTime)
    dw_created_at = Column(DateTime, server_default=func.now())
    dw_updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    orders = relationship("FactOrder", back_populates="user", lazy="dynamic")


class DimProduct(Base):
    """Product dimension table."""
    __tablename__ = "dim_products"

    product_sk = Column(BigInteger, primary_key=True, autoincrement=True)
    product_id = Column(String(36), nullable=False, unique=True, index=True)
    name = Column(String(500), nullable=False)
    category = Column(String(100), nullable=False, index=True)
    price = Column(Float, nullable=False)
    description = Column(Text)
    dw_created_at = Column(DateTime, server_default=func.now())
    dw_updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    orders = relationship("FactOrder", back_populates="product", lazy="dynamic")


class DimDate(Base):
    """Date dimension table for time-based analytics."""
    __tablename__ = "dim_date"

    date_sk = Column(Integer, primary_key=True)  # YYYYMMDD format
    full_date = Column(DateTime, nullable=False, unique=True)
    year = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    month_name = Column(String(20), nullable=False)
    week_of_year = Column(Integer, nullable=False)
    day_of_month = Column(Integer, nullable=False)
    day_of_week = Column(Integer, nullable=False)
    day_name = Column(String(20), nullable=False)
    is_weekend = Column(Integer, nullable=False)  # 0/1


# ── Fact Tables ────────────────────────────────────────────────────────────

class FactOrder(Base):
    """
    Central fact table for orders (star schema core).
    Grain: one row per order line item.
    """
    __tablename__ = "fact_orders"

    order_sk = Column(BigInteger, primary_key=True, autoincrement=True)
    order_id = Column(String(36), nullable=False, unique=True, index=True)
    user_sk = Column(BigInteger, ForeignKey("dim_users.user_sk"), nullable=False)
    product_sk = Column(BigInteger, ForeignKey("dim_products.product_sk"), nullable=False)
    date_sk = Column(Integer, ForeignKey("dim_date.date_sk"), nullable=False)
    quantity = Column(Integer, nullable=False)
    unit_price = Column(Float, nullable=False)
    total_amount = Column(Float, nullable=False)
    status = Column(String(50), nullable=False)
    order_time = Column(DateTime, nullable=False)
    dw_created_at = Column(DateTime, server_default=func.now())

    user = relationship("DimUser", back_populates="orders")
    product = relationship("DimProduct", back_populates="orders")


class FactEvent(Base):
    """Fact table for user events (clickstream)."""
    __tablename__ = "fact_events"

    event_sk = Column(BigInteger, primary_key=True, autoincrement=True)
    event_id = Column(String(36), nullable=False, unique=True, index=True)
    user_sk = Column(BigInteger, ForeignKey("dim_users.user_sk"), nullable=True)
    product_sk = Column(BigInteger, ForeignKey("dim_products.product_sk"), nullable=True)
    date_sk = Column(Integer, ForeignKey("dim_date.date_sk"), nullable=False)
    event_type = Column(String(50), nullable=False, index=True)
    session_id = Column(String(36))
    device_type = Column(String(20))
    page_url = Column(Text)
    referrer = Column(Text)
    timestamp = Column(DateTime, nullable=False)
    dw_created_at = Column(DateTime, server_default=func.now())
