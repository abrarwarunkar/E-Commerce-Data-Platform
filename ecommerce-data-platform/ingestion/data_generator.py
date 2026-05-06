"""
Data generator that simulates realistic e-commerce events.
Data Lineage: ingestion/data_generator.py -> kafka_producer.py -> Kafka topics
"""

import os
import random
import time
from datetime import datetime, timedelta
from typing import Generator, List, Tuple

from faker import Faker

from ingestion.schemas import (
    EventSchema,
    EventType,
    OrderSchema,
    OrderStatus,
    ProductCategory,
    ProductSchema,
    UserSchema,
)
from configs.logging_config import get_logger

logger = get_logger(__name__, "data_generator.log")
fake = Faker()
Faker.seed(42)

# ── Weighted event distribution (realistic traffic) ────────────────────────
EVENT_WEIGHTS: List[Tuple[EventType, float]] = [
    (EventType.PAGE_VIEW, 0.35),
    (EventType.PRODUCT_VIEW, 0.25),
    (EventType.SEARCH, 0.15),
    (EventType.ADD_TO_CART, 0.10),
    (EventType.WISHLIST_ADD, 0.05),
    (EventType.CHECKOUT_START, 0.05),
    (EventType.PURCHASE, 0.03),
    (EventType.REMOVE_FROM_CART, 0.02),
]

DEVICE_TYPES = ["desktop", "mobile", "tablet"]
DEVICE_WEIGHTS = [0.45, 0.45, 0.10]


class EcommerceDataGenerator:
    """
    Generates realistic synthetic e-commerce data.

    Maintains an in-memory pool of users and products to create
    realistic referential integrity between generated records.
    """

    def __init__(
        self,
        num_users: int = 1000,
        num_products: int = 500,
        events_per_second: float = 10.0,
    ) -> None:
        self.num_users = num_users
        self.num_products = num_products
        self.events_per_second = events_per_second
        self._sleep_interval = 1.0 / events_per_second

        self.users: List[UserSchema] = []
        self.products: List[ProductSchema] = []

        logger.info(
            "Initializing data generator: users=%d, products=%d, eps=%.1f",
            num_users,
            num_products,
            events_per_second,
        )
        self._initialize_pool()

    def _initialize_pool(self) -> None:
        """Create initial user and product pools."""
        logger.info("Generating %d users ...", self.num_users)
        self.users = [self._generate_user() for _ in range(self.num_users)]

        logger.info("Generating %d products ...", self.num_products)
        self.products = [self._generate_product() for _ in range(self.num_products)]

        logger.info("Data pool initialized successfully.")

    # ── Individual record generators ──────────────────────────────────────

    def _generate_user(self) -> UserSchema:
        """Generate a single user record."""
        return UserSchema(
            name=fake.name(),
            email=fake.email(),
            location=f"{fake.city()}, {fake.country_code()}",
        )

    def _generate_product(self) -> ProductSchema:
        """Generate a single product record."""
        category = random.choice(list(ProductCategory))
        price_ranges = {
            ProductCategory.ELECTRONICS: (50, 3000),
            ProductCategory.CLOTHING: (10, 500),
            ProductCategory.HOME_GARDEN: (5, 1000),
            ProductCategory.SPORTS: (20, 800),
            ProductCategory.BOOKS: (5, 100),
            ProductCategory.BEAUTY: (5, 300),
            ProductCategory.TOYS: (10, 200),
            ProductCategory.FOOD: (2, 100),
            ProductCategory.AUTOMOTIVE: (20, 5000),
            ProductCategory.JEWELRY: (50, 10000),
        }
        lo, hi = price_ranges.get(category, (10, 500))
        return ProductSchema(
            name=fake.catch_phrase(),
            category=category,
            price=round(random.uniform(lo, hi), 2),
            description=fake.text(max_nb_chars=200),
            stock_quantity=random.randint(0, 500),
        )

    def generate_event(self) -> EventSchema:
        """Generate a random user event with weighted type distribution."""
        user = random.choice(self.users)
        event_type = random.choices(
            [e for e, _ in EVENT_WEIGHTS],
            weights=[w for _, w in EVENT_WEIGHTS],
            k=1,
        )[0]

        product_id = None
        if event_type in (
            EventType.PRODUCT_VIEW,
            EventType.ADD_TO_CART,
            EventType.REMOVE_FROM_CART,
            EventType.PURCHASE,
            EventType.WISHLIST_ADD,
        ):
            product_id = random.choice(self.products).product_id

        return EventSchema(
            user_id=user.user_id,
            event_type=event_type,
            product_id=product_id,
            page_url=fake.uri(),
            referrer=fake.uri() if random.random() > 0.5 else None,
            device_type=random.choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS, k=1)[0],
        )

    def generate_order(self) -> OrderSchema:
        """Generate a random purchase order."""
        user = random.choice(self.users)
        product = random.choice(self.products)
        quantity = random.randint(1, 5)
        total = round(product.price * quantity, 2)

        # Simulate some historical orders (late-arriving data handling)
        order_time = datetime.utcnow() - timedelta(
            minutes=random.randint(0, 60)
        )

        return OrderSchema(
            user_id=user.user_id,
            product_id=product.product_id,
            quantity=quantity,
            unit_price=product.price,
            total_amount=total,
            status=random.choices(
                list(OrderStatus),
                weights=[0.15, 0.40, 0.25, 0.10, 0.05, 0.05],
                k=1,
            )[0],
            order_time=order_time,
        )

    # ── Stream generators ─────────────────────────────────────────────────

    def event_stream(self, max_events: int = 0) -> Generator[EventSchema, None, None]:
        """
        Infinite (or bounded) generator of user events.

        Args:
            max_events: Stop after this many events (0 = infinite)
        """
        count = 0
        while True:
            yield self.generate_event()
            count += 1
            if max_events and count >= max_events:
                logger.info("Reached max_events=%d, stopping event stream.", max_events)
                break
            time.sleep(self._sleep_interval)

    def mixed_stream(
        self,
        max_events: int = 0,
        order_ratio: float = 0.15,
    ) -> Generator[tuple, None, None]:
        """
        Yields (record_type, record) tuples mixing events and orders.

        Args:
            max_events: Stop after this many records (0 = infinite)
            order_ratio: Fraction of records that are orders
        """
        count = 0
        while True:
            if random.random() < order_ratio:
                yield ("order", self.generate_order())
            else:
                yield ("event", self.generate_event())

            count += 1
            if max_events and count >= max_events:
                logger.info("Reached max_events=%d, stopping mixed stream.", max_events)
                break
            time.sleep(self._sleep_interval)

    def get_all_users(self) -> List[UserSchema]:
        return self.users

    def get_all_products(self) -> List[ProductSchema]:
        return self.products


# ── Entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    events_per_second = float(os.getenv("EVENTS_PER_SECOND", "5"))
    num_users = int(os.getenv("NUM_USERS", "1000"))
    num_products = int(os.getenv("NUM_PRODUCTS", "500"))

    gen = EcommerceDataGenerator(num_users, num_products, events_per_second)

    logger.info("Starting data generation preview (10 events)...")
    for i, event in enumerate(gen.event_stream(max_events=10)):
        logger.info("Event[%d]: type=%s user=%s", i, event.event_type, event.user_id[:8])
