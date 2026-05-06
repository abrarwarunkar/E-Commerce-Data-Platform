"""
Users router — user activity and engagement endpoints.
Data Lineage: PostgreSQL fact_events + fact_orders + dim_users -> /api/v1/users
"""

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import get_db
from api.models import UserActivity, UserActivityResponse
from configs.logging_config import get_logger

logger = get_logger(__name__)
router = APIRouter()


@router.get("/activity", response_model=UserActivityResponse, summary="Get user activity metrics")
async def get_user_activity(
    limit: int = Query(default=20, ge=1, le=200),
    days: int = Query(default=7, ge=1, le=90),
    db: asyncpg.Connection = Depends(get_db),
):
    """Return top active users ranked by total spend in the last N days."""
    query = """
        SELECT
            du.user_id,
            du.name,
            du.email,
            du.location,
            COUNT(DISTINCT fe.event_id)   AS total_events,
            COUNT(DISTINCT fe.session_id) AS total_sessions,
            COUNT(DISTINCT fo.order_id)   AS total_orders,
            ROUND(COALESCE(SUM(fo.total_amount), 0)::numeric, 2) AS total_spend,
            ROUND(COALESCE(AVG(fo.total_amount), 0)::numeric, 2) AS avg_order_value,
            MAX(fe.timestamp)             AS last_seen
        FROM dim_users du
        LEFT JOIN fact_events fe ON du.user_sk = fe.user_sk
            AND fe.timestamp >= CURRENT_TIMESTAMP - ($1 || ' days')::interval
        LEFT JOIN fact_orders fo ON du.user_sk = fo.user_sk
            AND fo.order_time >= CURRENT_TIMESTAMP - ($1 || ' days')::interval
            AND fo.status != 'cancelled'
        GROUP BY du.user_id, du.name, du.email, du.location
        ORDER BY total_spend DESC
        LIMIT $2
    """
    rows = await db.fetch(query, str(days), limit)
    users = [UserActivity(**dict(r)) for r in rows]
    logger.info("user_activity: returned %d users (days=%d)", len(users), days)
    return UserActivityResponse(users=users, total=len(users))


@router.get("/{user_id}", response_model=UserActivity, summary="Get single user activity")
async def get_user(user_id: str, db: asyncpg.Connection = Depends(get_db)):
    """Return activity metrics for a specific user."""
    query = """
        SELECT
            du.user_id, du.name, du.email, du.location,
            COUNT(DISTINCT fe.event_id)   AS total_events,
            COUNT(DISTINCT fe.session_id) AS total_sessions,
            COUNT(DISTINCT fo.order_id)   AS total_orders,
            ROUND(COALESCE(SUM(fo.total_amount), 0)::numeric, 2) AS total_spend,
            ROUND(COALESCE(AVG(fo.total_amount), 0)::numeric, 2) AS avg_order_value,
            MAX(fe.timestamp) AS last_seen
        FROM dim_users du
        LEFT JOIN fact_events fe ON du.user_sk = fe.user_sk
        LEFT JOIN fact_orders fo ON du.user_sk = fo.user_sk AND fo.status != 'cancelled'
        WHERE du.user_id = $1
        GROUP BY du.user_id, du.name, du.email, du.location
    """
    row = await db.fetchrow(query, user_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found")
    return UserActivity(**dict(row))
