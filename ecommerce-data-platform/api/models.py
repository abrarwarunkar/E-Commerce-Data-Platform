"""
Pydantic response models for the API layer.
Data Lineage: api/models.py -> all API routers
"""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class TopProduct(BaseModel):
    product_id: str
    name: str
    category: str
    price: float
    total_revenue: float
    total_orders: int
    total_units_sold: int


class TopProductsResponse(BaseModel):
    products: List[TopProduct]
    total: int


class UserActivity(BaseModel):
    user_id: str
    name: Optional[str]
    email: Optional[str]
    location: Optional[str]
    total_events: Optional[int]
    total_sessions: Optional[int]
    total_orders: Optional[int]
    total_spend: Optional[float]
    avg_order_value: Optional[float]
    last_seen: Optional[datetime]


class UserActivityResponse(BaseModel):
    users: List[UserActivity]
    total: int


class DailyRevenue(BaseModel):
    order_date: str
    category: Optional[str]
    total_revenue: float
    total_orders: int
    avg_order_value: float


class SalesMetrics(BaseModel):
    total_revenue: float
    total_orders: int
    total_users: int
    avg_order_value: float
    top_category: Optional[str]


class SalesTrend(BaseModel):
    period: str
    revenue: float
    orders: int


class CategoryBreakdown(BaseModel):
    category: str
    total_revenue: float
    total_orders: int
    revenue_share_pct: float
