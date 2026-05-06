"""
Products router — top products and product performance endpoints.
Data Lineage: PostgreSQL fact_orders + dim_products -> /api/v1/products
"""

from typing import Optional

import asyncpg
from fastapi import APIRouter, Depends, Query

from api.dependencies import get_db
from api.models import TopProduct, TopProductsResponse
from configs.logging_config import get_logger

logger = get_logger(__name__)
router = APIRouter()


@router.get("/top", response_model=TopProductsResponse, summary="Get top products by revenue")
async def get_top_products(
    limit: int = Query(default=10, ge=1, le=100, description="Number of top products to return"),
    category: Optional[str] = Query(default=None, description="Filter by product category"),
    days: int = Query(default=30, ge=1, le=365, description="Lookback window in days"),
    db: asyncpg.Connection = Depends(get_db),
):
    """
    Return the top N products by total revenue.

    Queries fact_orders joined with dim_products filtered to the last N days.
    Supports optional category filtering.
    """
    category_filter = "AND dp.category = $3" if category else ""
    params = [days, limit]
    if category:
        params.append(category)

    query = f"""
        SELECT
            dp.product_id,
            dp.name,
            dp.category,
            dp.price,
            ROUND(SUM(fo.total_amount)::numeric, 2)  AS total_revenue,
            COUNT(fo.order_id)                         AS total_orders,
            SUM(fo.quantity)                           AS total_units_sold
        FROM fact_orders fo
        JOIN dim_products dp ON fo.product_sk = dp.product_sk
        JOIN dim_date dd     ON fo.date_sk    = dd.date_sk
        WHERE fo.status != 'cancelled'
          AND dd.full_date >= CURRENT_DATE - INTERVAL '$1 days'
          {category_filter}
        GROUP BY dp.product_id, dp.name, dp.category, dp.price
        ORDER BY total_revenue DESC
        LIMIT $2
    """

    rows = await db.fetch(query, *params)
    products = [TopProduct(**dict(r)) for r in rows]
    logger.info("top_products: returned %d products (days=%d)", len(products), days)
    return TopProductsResponse(products=products, total=len(products))


@router.get("/categories", summary="Get distinct product categories")
async def get_categories(db: asyncpg.Connection = Depends(get_db)):
    """Return all distinct product categories available in the warehouse."""
    rows = await db.fetch(
        "SELECT DISTINCT category FROM dim_products ORDER BY category"
    )
    return {"categories": [r["category"] for r in rows]}
