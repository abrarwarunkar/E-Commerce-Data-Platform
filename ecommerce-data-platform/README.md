# E-Commerce Data Platform

A production-ready, end-to-end data engineering platform that ingests, processes, transforms, and serves e-commerce data using modern data engineering practices. The platform implements a medallion architecture (Bronze/Silver/Gold) with real-time streaming and batch processing capabilities.

## Overview

This platform demonstrates a complete data pipeline for e-commerce analytics:

- **Real-time Data Ingestion**: Continuous event streaming from multiple Kafka topics
- **Medallion Architecture**: Bronze (raw) → Silver (cleaned) → Gold (aggregated) data layers
- **Data Warehouse**: PostgreSQL star schema for analytical queries
- **Business Transformations**: dbt models for customer analytics, product performance, and sales trends
- **REST API**: FastAPI serving layer for data access
- **Analytics Dashboard**: Streamlit visualization with Plotly charts

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                           E-COMMERCE DATA PLATFORM                                 │
│                                                                                 │
│  ┌──────────────────┐    ┌──────────────────┐    ┌───────────────────────┐  │
│  │ Data Generator   │───▶│  Kafka Broker    │───▶│  Spark Structured     │  │
│  │ (Python/Faker)  │    │  (4 topics)       │    │  Streaming            │  │
│  └──────────────────┘    └──────────────────┘    └───────────┬───────────┘  │
│                                                               │               │
│                                                   ┌───────────▼────────────┐  │
│                                                   │  MinIO (Data Lake)    │  │
│                                                   │  ┌────────────────┐  │  │
│                                                   │  │ Bronze Layer  │  │  │
│                                                   │  │ Silver Layer  │  │  │
│                                                   │  │ Gold Layer   │  │  │
│                                                   │  └────────────────┘  │  │
│                                                   └───────────┬───────────┘  │
│                                                               │               │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────▼────────────┐  │
│  │ Apache Airflow  │───▶│  Spark Batch     │───▶│   PostgreSQL DWH    │  │
│  │ (Orchestrator) │    │  (PySpark Jobs)  │    │   (Star Schema)      │  │
│  └──────────────────┘    └──────────────────┘    └───────────┬───────────┘  │
│                                                               │               │
│                                                   ┌───────────▼────────────┐  │
│                                                   │  dbt (Transformations)│  │
│                                                   │  ┌─────────────────┐  │  │
│                                                   │  │ staging/       │  │  │
│                                                   │  │ marts/         │  │  │
│                                                   │  └─────────────────┘  │  │
│                                                   └───────────┬───────────┘  │
│                                   ┌───────────────────────────┤               │
│                                   │                           │               │
│               ┌───────────────────▼────┐        ┌────────────▼────────────┐  │
│               │  FastAPI (REST)        │        │  Streamlit Dashboard   │  │
│               │  /api/v1/...           │        │  (Charts & KPIs)      │  │
│               └─────────────────────────┘        └───────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Tech Stack

| Layer | Technology | Version |
|-------|------------|---------|
| Language | Python | 3.11 |
| Streaming | Apache Kafka (Confluent) | 7.6.1 |
| Stream Processing | Apache Spark | 3.5.1 |
| Batch Processing | PySpark | 3.5.1 |
| Orchestration | Apache Airflow | 2.9.1 |
| Data Lake | MinIO (S3-compatible) | 2024-05-01 |
| Data Warehouse | PostgreSQL | 16 |
| Transformation | dbt-core | 1.8.2 |
| API | FastAPI | 0.111.0 |
| Dashboard | Streamlit | 1.35.0 |
| Visualization | Plotly | 5.22.0 |
| Containerization | Docker + Compose | 2.20+ |

## Data Model

### Source Entities

| Entity | Description | Key Columns |
|--------|------------|------------|
| Users | Customer dimension | user_id, name, email, location, created_at |
| Products | Product catalog | product_id, name, category, price, stock_quantity |
| Orders | Transaction records | order_id, user_id, product_id, quantity, total_amount, status |
| Events | User activity log | event_id, user_id, event_type, product_id, session_id, timestamp |

### Event Types

Weighted distribution for realistic traffic simulation:
- PAGE_VIEW: 35%
- PRODUCT_VIEW: 25%
- SEARCH: 15%
- ADD_TO_CART: 10%
- CHECKOUT_START: 5%
- WISHLIST_ADD: 5%
- PURCHASE: 3%
- REMOVE_FROM_CART: 2%

### Product Categories

Electronics, Clothing, Home & Garden, Sports, Books, Beauty, Toys, Food, Automotive, Jewelry

### Star Schema (PostgreSQL DWH)

```
                  ┌──────────────┐
                  │   dim_date   │
                  └──────┬───────┘
                         │
     ┌───────────┐   ┌───▼──────────┐   ┌────────────────┐
     │ dim_users │──▶│  fact_orders │◀──│  dim_products  │
     └───────────┘   └──────────────┘   └────────────────┘
          │
          └──────────────▶ fact_events
```

### dbt Mart Models

| Model | Description |
|-------|-------------|
| customer_analytics | RFM segmentation, churn risk, lifetime value |
| product_performance | Revenue rank, return rate, unique buyers |
| sales_trends | Day-over-day, Week-over-week, Month-over-month growth metrics |

## Data Flow

```
1. DATA GENERATION (ingestion/data_generator.py)
   - Simulates 10 events/sec with weighted distributions
   - Maintains user/product pools for referential integrity

2. KAFKA INGESTION (ingestion/kafka_producer.py)
   - Topics: user-events, orders, products, users
   - Idempotent delivery with snappy compression

3. SPARK STREAMING - BRONZE LAYER (streaming/spark_streaming.py)
   - Multi-topic Kafka consumer
   - Validates data with stream_validator.py
   - Writes Parquet partitioned by year/month/day/hour

4. BATCH PROCESSING - SILVER LAYER (batch/silver_processor.py)
   - Deduplication, type casting, value normalization
   - Runs daily via Airflow DAG

5. AGGREGATION - GOLD LAYER (batch/gold_aggregator.py)
   - Daily revenue, top products, user activity
   - Business-level aggregations

6. WAREHOUSE LOAD (warehouse/postgres_loader.py)
   - Upserts to dimension tables
   - Inserts to fact tables with surrogate key resolution

7. DBT TRANSFORMATIONS
   - staging/: Views over warehouse tables
   - marts/: Incremental business intelligence models

8. SERVING LAYER
   - api/main.py: FastAPI REST endpoints
   - dashboard/app.py: Streamlit visualizations
```

## Quick Start

### Prerequisites

- Docker Desktop >= 24.0
- Docker Compose >= 2.20
- 8GB RAM minimum (16GB recommended)

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd ecommerce-data-platform
```

2. Copy environment configuration:
```bash
cp .env.example .env
```

3. Start all services:
```bash
docker-compose up -d
```

This starts 13 services. Verify with:
```bash
docker-compose ps
```

### Service Endpoints

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:8080 | - |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Spark Master | http://localhost:8082 | - |
| Airflow | http://localhost:8083 | admin / admin |
| FastAPI Docs | http://localhost:8000/docs | - |
| Dashboard | http://localhost:8501 | - |
| PostgreSQL | localhost:5432 | ecommerce / ecommerce_secret |

### Trigger Pipeline

1. Run batch processing:
```bash
docker-compose exec streaming python -m batch.silver_processor
docker-compose exec streaming python -m batch.gold_aggregator
```

2. Run dbt transformations:
```bash
docker-compose exec airflow-webserver bash -c "
  cd /opt/airflow/dbt_models && \
  dbt deps --profiles-dir . && \
  dbt run --profiles-dir .
"
```

### Run Tests

```bash
# Install dependencies
pip install -r requirements.txt

# Run unit tests
pytest tests/unit/ -v

# With coverage
pytest tests/ --cov=. --cov-report=html
```

## Project Structure

```
ecommerce-data-platform/
├── ingestion/                    # Data generation + Kafka producer
│   ├── schemas.py               # Pydantic entity schemas
│   ├── data_generator.py        # Synthetic data generator
│   └── kafka_producer.py        # Kafka producer with retries
├── streaming/                   # Spark Structured Streaming
│   ├── spark_streaming.py       # Multi-topic consumer → bronze
│   └── stream_validator.py      # Data validation rules
├── batch/                       # PySpark batch jobs
│   ├── spark_session.py         # SparkSession factory
│   ├── silver_processor.py     # Bronze → Silver
│   └── gold_aggregator.py      # Silver → Gold
├── warehouse/                  # PostgreSQL data warehouse
│   ├── models.py               # SQLAlchemy models
│   └── postgres_loader.py     # Upsert loader
├── dbt_models/                 # dbt transformation layer
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/             # Views
│       └── marts/             # Business models
├── api/                        # FastAPI REST layer
│   ├── main.py               # App entrypoint
│   ├── dependencies.py      # DB pool management
│   ├── models.py           # Response schemas
│   └── routers/
│       ├── products.py      # Product endpoints
│       ├── users.py        # User endpoints
│       └── metrics.py      # Metrics endpoints
├── dashboard/                 # Streamlit dashboard
│   └── app.py              # Analytics visualizations
├── airflow/
│   └── dags/
│       ├── batch_pipeline_dag.py    # Daily ETL
│       └── dbt_transform_dag.py    # dbt runs
├── docker/
│   ├── Dockerfile.ingestion
│   ├── Dockerfile.streaming
│   ├── Dockerfile.api
│   ├── Dockerfile.dashboard
│   └── init-postgres.sh
├── configs/
│   ├── kafka_config.py
│   ├── spark_config.py
│   └── logging_config.py
├── tests/
│   └── unit/
│       ├── test_data_generator.py
│       ├── test_api.py
│       └── test_validators.py
├── .env.example
├── docker-compose.yml
├── requirements.txt
└── pytest.ini
```

## API Endpoints

### Products

- `GET /api/v1/products/top` - Top products by revenue
- `GET /api/v1/products/categories` - Product categories with revenue

### Users

- `GET /api/v1/users/{user_id}` - User details
- `GET /api/v1/users/activity` - User activity metrics

### Metrics

- `GET /api/v1/metrics/summary` - Overall summary KPIs
- `GET /api/v1/metrics/revenue/trends` - Revenue time series
- `GET /api/v1/metrics/categories` - Category breakdown

## Production Features

| Feature | Implementation |
|---------|----------------|
| Logging | Rotating file + colored console |
| Retries | Exponential backoff (tenacity) |
| Data Validation | Spark + Pydantic schemas |
| Idempotency | Kafka idempotent producer, PostgreSQL upserts |
| Incremental Processing | Date-partitioned layers, dbt incremental models |
| Schema Enforcement | Spark StructType, Pydantic validators |
| Environment Config | .env files throughout |

## Stopping the Platform

```bash
# Stop services
docker-compose down

# Full cleanup (removes volumes)
docker-compose down -v
```

## License

MIT