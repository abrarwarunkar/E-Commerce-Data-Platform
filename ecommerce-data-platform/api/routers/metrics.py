"""
Metrics router — sales KPIs, revenue trends, and category breakdown.
Data Lineage: PostgreSQL fact_orders + dim_* -> /api/v1/metrics
"""

import asyncpg
from fastapi import APIRouter, Depends, Query

from api.dependencies import get_db
from api.models import CategoryBreakdown, DailyRevenue, SalesMetrics, SalesTrend
from configs.logging_config import get_logger

logger = get_logger(__name__)
router = APIRouter()


@router.get("/summary", response_model=SalesMetrics, summary="Overall sales summary KPIs")
async def get_sales_summary(
    days: int = Query(default=30, ge=1, le=365),
    db: asyncpg.Connection = Depends(get_db),
):
    """Return high-level KPIs: total revenue, orders, unique customers, AOV."""
    row = await db.fetchrow(
        """
        SELECT
            ROUND(SUM(fo.total_amount)::numeric, 2)  AS total_revenue,
            COUNT(fo.order_id)                         AS total_orders,
            COUNT(DISTINCT fo.user_sk)                 AS total_users,
            ROUND(AVG(fo.total_amount)::numeric, 2)   AS avg_order_value
        FROM fact_orders fo
        JOIN dim_date dd ON fo.date_sk = dd.date_sk
        WHERE fo.status != 'cancelled'
          AND dd.full_date >= CURRENT_DATE - ($1 || ' days')::interval
        """,
        str(days),
    )
    top_cat = await db.fetchval(
        """
        SELECT dp.category
        FROM fact_orders fo
        JOIN dim_products dp ON fo.product_sk = dp.product_sk
        JOIN dim_date dd ON fo.date_sk = dd.date_sk
        WHERE fo.status != 'cancelled'
          AND dd.full_date >= CURRENT_DATE - ($1 || ' days')::interval
        GROUP BY dp.category
        ORDER BY SUM(fo.total_amount) DESC
        LIMIT 1
        """,
        str(days),
    )
    return SalesMetrics(
        total_revenue=row["total_revenue"] or 0.0,
        total_orders=row["total_orders"] or 0,
        total_users=row["total_users"] or 0,
        avg_order_value=row["avg_order_value"] or 0.0,
        top_category=top_cat,
    )


@router.get("/revenue/daily", response_model=list[DailyRevenue], summary="Daily revenue breakdown")
async def get_daily_revenue(
    days: int = Query(default=30, ge=1, le=365),
    db: asyncpg.Connection = Depends(get_db),
):
    """Return daily revenue aggregated by product category."""
    rows = await db.fetch(
        """
        SELECT
            dd.full_date::date::text              AS order_date,
            dp.category,
            ROUND(SUM(fo.total_amount)::numeric, 2) AS total_revenue,
            COUNT(fo.order_id)                     AS total_orders,
            ROUND(AVG(fo.total_amount)::numeric, 2) AS avg_order_value
        FROM fact_orders fo
        JOIN dim_date    dd ON fo.date_sk    = dd.date_sk
        JOIN dim_products dp ON fo.product_sk = dp.product_sk
        WHERE fo.status != 'cancelled'
          AND dd.full_date >= CURRENT_DATE - ($1 || ' days')::interval
        GROUP BY dd.full_date, dp.category
        ORDER BY dd.full_date DESC, total_revenue DESC
        """,
        str(days),
    )
    return [DailyRevenue(**dict(r)) for r in rows]


@router.get("/revenue/trends", response_model=list[SalesTrend], summary="Revenue trend over time")
async def get_revenue_trends(
    granularity: str = Query(default="day", pattern="^(hour|day|week|month)$"),
    days: int = Query(default=30, ge=1, le=365),
    db: asyncpg.Connection = Depends(get_db),
):
    """Return revenue trends grouped by specified time granularity."""
    trunc_map = {"hour": "hour", "day": "day", "week": "week", "month": "month"}
    trunc = trunc_map[granularity]
    rows = await db.fetch(
        f"""
        SELECT
            DATE_TRUNC('{trunc}', fo.order_time)::text AS period,
            ROUND(SUM(fo.total_amount)::numeric, 2)    AS revenue,
            COUNT(fo.order_id)                          AS orders
        FROM fact_orders fo
        JOIN dim_date dd ON fo.date_sk = dd.date_sk
        WHERE fo.status != 'cancelled'
          AND dd.full_date >= CURRENT_DATE - ($1 || ' days')::interval
        GROUP BY DATE_TRUNC('{trunc}', fo.order_time)
        ORDER BY period ASC
        """,
        str(days),
    )
    return [SalesTrend(**dict(r)) for r in rows]


@router.get("/categories", response_model=list[CategoryBreakdown], summary="Revenue by category")
async def get_category_breakdown(
    days: int = Query(default=30, ge=1, le=365),
    db: asyncpg.Connection = Depends(get_db),
):
    """Return revenue share by product category."""
    rows = await db.fetch(
        """
        WITH category_totals AS (
            SELECT
                dp.category,
                ROUND(SUM(fo.total_amount)::numeric, 2) AS total_revenue,
                COUNT(fo.order_id) AS total_orders
            FROM fact_orders fo
            JOIN dim_products dp ON fo.product_sk = dp.product_sk
            JOIN dim_date dd     ON fo.date_sk     = dd.date_sk
            WHERE fo.status != 'cancelled'
              AND dd.full_date >= CURRENT_DATE - ($1 || ' days')::interval
            GROUP BY dp.category
        ),
        grand_total AS (SELECT SUM(total_revenue) AS grand FROM category_totals)
        SELECT
            ct.category,
            ct.total_revenue,
            ct.total_orders,
            ROUND((ct.total_revenue / gt.grand * 100)::numeric, 2) AS revenue_share_pct
        FROM category_totals ct, grand_total gt
        ORDER BY total_revenue DESC
        """,
        str(days),
    )
    return [CategoryBreakdown(**dict(r)) for r in rows]
