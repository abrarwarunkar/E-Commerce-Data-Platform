"""
Unit tests for the FastAPI endpoints.
Uses TestClient to test all routers without a live database.
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, MagicMock, patch

from api.main import app


@pytest.fixture
def client():
    """Create test client with mocked DB pool."""
    with patch("api.dependencies._pool") as mock_pool:
        # Mock the acquire context manager
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        with TestClient(app) as c:
            yield c, mock_conn


class TestHealthEndpoints:
    def test_root(self, client):
        c, _ = client
        resp = c.get("/")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_health(self, client):
        c, _ = client
        resp = c.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "healthy"


class TestProductsEndpoints:
    def test_top_products_default(self, client):
        c, mock_conn = client
        mock_conn.fetch = AsyncMock(return_value=[
            {
                "product_id": "p1", "name": "Widget A", "category": "electronics",
                "price": 99.99, "total_revenue": 5000.0, "total_orders": 50,
                "total_units_sold": 75,
            }
        ])
        resp = c.get("/api/v1/products/top")
        assert resp.status_code == 200
        data = resp.json()
        assert "products" in data
        assert data["total"] == 1

    def test_top_products_with_category(self, client):
        c, mock_conn = client
        mock_conn.fetch = AsyncMock(return_value=[])
        resp = c.get("/api/v1/products/top?category=electronics&limit=5")
        assert resp.status_code == 200

    def test_categories(self, client):
        c, mock_conn = client
        mock_conn.fetch = AsyncMock(return_value=[
            {"category": "electronics"}, {"category": "clothing"}
        ])
        resp = c.get("/api/v1/products/categories")
        assert resp.status_code == 200
        assert "categories" in resp.json()


class TestMetricsEndpoints:
    def test_summary(self, client):
        c, mock_conn = client
        mock_conn.fetchrow = AsyncMock(return_value={
            "total_revenue": 100000.0,
            "total_orders": 500,
            "total_users": 200,
            "avg_order_value": 200.0,
        })
        mock_conn.fetchval = AsyncMock(return_value="electronics")
        resp = c.get("/api/v1/metrics/summary")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_revenue"] == 100000.0
        assert data["top_category"] == "electronics"

    def test_daily_revenue(self, client):
        c, mock_conn = client
        mock_conn.fetch = AsyncMock(return_value=[
            {
                "order_date": "2024-01-15", "category": "electronics",
                "total_revenue": 5000.0, "total_orders": 25, "avg_order_value": 200.0,
            }
        ])
        resp = c.get("/api/v1/metrics/revenue/daily?days=7")
        assert resp.status_code == 200

    def test_trends_invalid_granularity(self, client):
        c, _ = client
        resp = c.get("/api/v1/metrics/revenue/trends?granularity=yearly")
        assert resp.status_code == 422


class TestUsersEndpoints:
    def test_user_activity(self, client):
        c, mock_conn = client
        mock_conn.fetch = AsyncMock(return_value=[
            {
                "user_id": "u1", "name": "Alice", "email": "a@b.com",
                "location": "NYC", "total_events": 50, "total_sessions": 10,
                "total_orders": 5, "total_spend": 500.0,
                "avg_order_value": 100.0, "last_seen": None,
            }
        ])
        resp = c.get("/api/v1/users/activity")
        assert resp.status_code == 200

    def test_user_not_found(self, client):
        c, mock_conn = client
        mock_conn.fetchrow = AsyncMock(return_value=None)
        resp = c.get("/api/v1/users/nonexistent-uuid")
        assert resp.status_code == 404
