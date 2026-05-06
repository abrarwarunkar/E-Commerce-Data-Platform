"""
Unit tests for data generator and schemas.
Data Lineage: tests/unit/test_data_generator.py
"""

import pytest
from datetime import datetime

from ingestion.data_generator import EcommerceDataGenerator
from ingestion.schemas import (
    EventSchema, EventType, OrderSchema, OrderStatus,
    ProductCategory, ProductSchema, UserSchema,
)


@pytest.fixture(scope="module")
def generator():
    return EcommerceDataGenerator(num_users=50, num_products=20, events_per_second=1000)


class TestUserSchema:
    def test_valid_user(self):
        user = UserSchema(name="Alice", email="alice@example.com", location="New York, US")
        assert user.user_id is not None
        assert user.email == "alice@example.com"

    def test_email_normalized(self):
        user = UserSchema(name="Bob", email="BOB@EXAMPLE.COM", location="London, UK")
        assert user.email == "bob@example.com"

    def test_invalid_email_raises(self):
        with pytest.raises(Exception):
            UserSchema(name="X", email="not-an-email", location="NYC")

    def test_created_at_auto(self):
        user = UserSchema(name="Carol", email="c@x.com", location="Paris, FR")
        assert isinstance(user.created_at, datetime)


class TestProductSchema:
    def test_valid_product(self):
        p = ProductSchema(name="Test Item", category=ProductCategory.ELECTRONICS, price=99.99)
        assert p.product_id is not None
        assert p.price == 99.99

    def test_price_rounded(self):
        p = ProductSchema(name="Item", category=ProductCategory.BOOKS, price=9.999)
        assert p.price == 10.0

    def test_invalid_price_raises(self):
        with pytest.raises(Exception):
            ProductSchema(name="Item", category=ProductCategory.BOOKS, price=-5)

    def test_price_too_high_raises(self):
        with pytest.raises(Exception):
            ProductSchema(name="Item", category=ProductCategory.BOOKS, price=200_000)


class TestEventSchema:
    def test_valid_event(self):
        e = EventSchema(user_id="user-123", event_type=EventType.PAGE_VIEW)
        assert e.event_id is not None
        assert e.product_id is None

    def test_purchase_event_with_product(self):
        e = EventSchema(
            user_id="u1",
            event_type=EventType.PURCHASE,
            product_id="p1",
            device_type="mobile",
        )
        assert e.device_type == "mobile"

    def test_invalid_device_type(self):
        with pytest.raises(Exception):
            EventSchema(user_id="u1", event_type=EventType.PAGE_VIEW, device_type="smartwatch")


class TestOrderSchema:
    def test_valid_order(self):
        o = OrderSchema(
            user_id="u1", product_id="p1",
            quantity=2, unit_price=50.0, total_amount=100.0,
        )
        assert o.status == OrderStatus.PENDING
        assert o.total_amount == 100.0

    def test_invalid_quantity(self):
        with pytest.raises(Exception):
            OrderSchema(user_id="u1", product_id="p1", quantity=0, unit_price=10, total_amount=0)


class TestDataGenerator:
    def test_pool_size(self, generator):
        assert len(generator.users) == 50
        assert len(generator.products) == 20

    def test_generate_event_returns_valid(self, generator):
        event = generator.generate_event()
        assert isinstance(event, EventSchema)
        assert event.user_id in {u.user_id for u in generator.users}

    def test_generate_order_returns_valid(self, generator):
        order = generator.generate_order()
        assert isinstance(order, OrderSchema)
        assert order.total_amount > 0

    def test_event_stream_bounded(self, generator):
        events = list(generator.event_stream(max_events=5))
        assert len(events) == 5

    def test_mixed_stream_bounded(self, generator):
        records = list(generator.mixed_stream(max_events=10))
        assert len(records) == 10
        for rtype, record in records:
            assert rtype in ("event", "order")

    def test_event_type_distribution(self, generator):
        """Purchase events should be rare (< 10% of sample)."""
        events = [generator.generate_event() for _ in range(200)]
        purchases = sum(1 for e in events if e.event_type == EventType.PURCHASE)
        assert purchases / 200 < 0.10
