"""
Pydantic schemas for all E-Commerce data entities.
Data Lineage: ingestion/schemas.py -> kafka_producer, streaming, batch
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, EmailStr, Field, field_validator


# ── Enums ──────────────────────────────────────────────────────────────────

class EventType(str, Enum):
    """User event types tracked on the platform."""
    PAGE_VIEW = "page_view"
    PRODUCT_VIEW = "product_view"
    ADD_TO_CART = "add_to_cart"
    REMOVE_FROM_CART = "remove_from_cart"
    CHECKOUT_START = "checkout_start"
    PURCHASE = "purchase"
    SEARCH = "search"
    WISHLIST_ADD = "wishlist_add"


class ProductCategory(str, Enum):
    """Available product categories."""
    ELECTRONICS = "electronics"
    CLOTHING = "clothing"
    HOME_GARDEN = "home_garden"
    SPORTS = "sports"
    BOOKS = "books"
    BEAUTY = "beauty"
    TOYS = "toys"
    FOOD = "food"
    AUTOMOTIVE = "automotive"
    JEWELRY = "jewelry"


class OrderStatus(str, Enum):
    """Order lifecycle statuses."""
    PENDING = "pending"
    CONFIRMED = "confirmed"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"
    RETURNED = "returned"


# ── Base Models ────────────────────────────────────────────────────────────

class UserSchema(BaseModel):
    """User entity schema."""
    user_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(..., min_length=1, max_length=255)
    email: str = Field(..., description="User email address")
    location: str = Field(..., min_length=1, max_length=255)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        if "@" not in v or "." not in v.split("@")[-1]:
            raise ValueError(f"Invalid email address: {v}")
        return v.lower().strip()

    model_config = {"json_encoders": {datetime: lambda v: v.isoformat()}}


class ProductSchema(BaseModel):
    """Product entity schema."""
    product_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(..., min_length=1, max_length=500)
    category: ProductCategory
    price: float = Field(..., gt=0, le=100_000)
    description: Optional[str] = Field(None, max_length=2000)
    stock_quantity: int = Field(default=100, ge=0)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    @field_validator("price")
    @classmethod
    def round_price(cls, v: float) -> float:
        return round(v, 2)

    model_config = {"json_encoders": {datetime: lambda v: v.isoformat()}}


class OrderSchema(BaseModel):
    """Order entity schema."""
    order_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    product_id: str
    quantity: int = Field(..., gt=0, le=1000)
    unit_price: float = Field(..., gt=0)
    total_amount: float = Field(..., gt=0)
    status: OrderStatus = Field(default=OrderStatus.PENDING)
    order_time: datetime = Field(default_factory=datetime.utcnow)

    @field_validator("total_amount")
    @classmethod
    def validate_total(cls, v: float, info) -> float:
        return round(v, 2)

    model_config = {"json_encoders": {datetime: lambda v: v.isoformat()}}


class EventSchema(BaseModel):
    """User event entity schema."""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    event_type: EventType
    product_id: Optional[str] = None
    session_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    page_url: Optional[str] = None
    referrer: Optional[str] = None
    device_type: Optional[str] = Field(None, pattern="^(desktop|mobile|tablet)$")
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    model_config = {"json_encoders": {datetime: lambda v: v.isoformat()}}


# ── Kafka Message Envelopes ────────────────────────────────────────────────

class KafkaMessage(BaseModel):
    """Generic Kafka message envelope."""
    topic: str
    key: str
    value: dict
    headers: dict = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
