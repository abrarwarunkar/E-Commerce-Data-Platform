# 🛒 E-Commerce Data Platform

A **production-ready, end-to-end data engineering platform** that ingests, streams, processes, transforms, and serves e-commerce data using modern data engineering practices.

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                      E-COMMERCE DATA PLATFORM                            │
│                                                                          │
│  ┌─────────────────┐   ┌─────────────────┐   ┌───────────────────────┐  │
│  │  Data Generator │──▶│  Kafka Broker   │──▶│  Spark Structured     │  │
│  │  (Python/Faker) │   │  (4 topics)     │   │  Streaming            │  │
│  └─────────────────┘   └─────────────────┘   └──────────┬────────────┘  │
│                                                          │               │
│                                              ┌───────────▼────────────┐  │
│                                              │  MinIO  (Data Lake)    │  │
│                                              │  ┌────────────────┐    │  │
│                                              │  │ 🥉 Bronze Layer│    │  │
│                                              │  │ 🥈 Silver Layer│    │  │
│                                              │  │ 🥇 Gold Layer  │    │  │
│                                              │  └────────────────┘    │  │
│                                              └───────────┬────────────┘  │
│                                                          │               │
│  ┌─────────────────┐   ┌─────────────────┐   ┌──────────▼────────────┐  │
│  │ Apache Airflow  │──▶│  Spark Batch    │──▶│   PostgreSQL DWH      │  │
│  │ (Orchestrator)  │   │  (PySpark Jobs) │   │   (Star Schema)       │  │
│  └─────────────────┘   └─────────────────┘   └──────────┬────────────┘  │
│                                                          │               │
│                                              ┌───────────▼────────────┐  │
│                                              │  dbt (Transformations) │  │
│                                              │  ┌─────────────────┐   │  │
│                                              │  │ staging/        │   │  │
│                                              │  │ marts/          │   │  │
│                                              │  └─────────────────┘   │  │
│                                              └───────────┬────────────┘  │
│                              ┌───────────────────────────┤               │
│                              │                           │               │
│              ┌───────────────▼───┐           ┌───────────▼────────────┐  │
│              │  FastAPI (REST)   │           │  Streamlit Dashboard   │  │
│              │  /api/v1/...      │           │  (Charts & KPIs)       │  │
│              └───────────────────┘           └────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 📊 Data Model

### Source Tables
| Entity | Key Columns | Layer |
|--------|-------------|-------|
| `users` | user_id, name, email, location, created_at | Dim |
| `products` | product_id, name, category, price | Dim |
| `orders` | order_id, user_id, product_id, quantity, total_amount, status | Fact |
| `events` | event_id, user_id, event_type, product_id, session_id, timestamp | Fact |

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
| `customer_analytics` | RFM segmentation, churn risk, lifetime value |
| `product_performance` | Revenue rank, return rate, unique buyers |
| `sales_trends` | DoD, WoW, MoM growth metrics |

---

## 🔄 Data Flow

```
1. DATA GENERATION
   ingestion/data_generator.py
   → Simulates 10 events/sec (page_views, purchases, orders)
   → Weighted distribution: page_view (35%), purchase (3%), etc.

2. KAFKA INGESTION
   ingestion/kafka_producer.py
   → Topics: user-events, orders, products, users
   → Idempotent delivery, snappy compression

3. SPARK STREAMING (Bronze)
   streaming/spark_streaming.py
   → Consumes all Kafka topics concurrently
   → Validates with stream_validator.py
   → Writes Parquet partitioned by year/month/day/hour to MinIO

4. BATCH PROCESSING (Silver)
   batch/silver_processor.py
   → Runs daily at 3AM UTC via Airflow
   → Deduplicates, casts types, normalizes values
   → Writes cleaned Parquet to MinIO silver/

5. AGGREGATION (Gold)
   batch/gold_aggregator.py
   → daily_revenue, top_products, user_activity, sales_trends
   → Writes to MinIO gold/

6. WAREHOUSE LOAD
   warehouse/postgres_loader.py
   → Upserts into dim_users, dim_products
   → Inserts into fact_orders, fact_events
   → Populates dim_date dimension

7. DBT TRANSFORMS
   dbt_models/models/
   → staging/ views over warehouse tables
   → marts/ incremental tables with business metrics

8. SERVING
   api/main.py → FastAPI REST endpoints
   dashboard/app.py → Streamlit visualization
```

---

## ⚙️ Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.11 |
| Streaming | Apache Kafka (Confluent) + Spark Structured Streaming |
| Batch Processing | Apache Spark 3.5 (PySpark) |
| Orchestration | Apache Airflow 2.9 |
| Data Lake | MinIO (S3-compatible) |
| Data Warehouse | PostgreSQL 16 |
| Transformation | dbt-core 1.8 |
| API | FastAPI + asyncpg |
| Dashboard | Streamlit + Plotly |
| Containerization | Docker + Docker Compose |
| Testing | pytest + pytest-asyncio |

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop ≥ 24.0
- Docker Compose ≥ 2.20
- 8GB RAM minimum (16GB recommended)

### 1. Clone and Configure

```bash
git clone <your-repo>
cd ecommerce-data-platform

# Copy environment file
cp .env.example .env
# Edit .env if needed (defaults work out of the box)
```

### 2. Start All Services

```bash
docker-compose up -d
```

This starts 13 services. Watch them come up:

```bash
docker-compose ps
docker-compose logs -f ingestion
```

### 3. Verify Services Are Running

| Service | URL | Credentials |
|---------|-----|-------------|
| **Kafka UI** | http://localhost:8080 | — |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |
| **Spark Master** | http://localhost:8082 | — |
| **Airflow** | http://localhost:8083 | admin / admin |
| **FastAPI Docs** | http://localhost:8000/docs | — |
| **Dashboard** | http://localhost:8501 | — |
| **PostgreSQL** | localhost:5432 | ecommerce / ecommerce_secret |

### 4. Trigger the Batch Pipeline Manually

```bash
# Run silver processing for today
docker-compose exec airflow-webserver \
  airflow dags trigger ecommerce_batch_pipeline

# Or run directly
docker-compose exec streaming python -m batch.silver_processor
docker-compose exec streaming python -m batch.gold_aggregator
```

### 5. Run dbt Transformations

```bash
docker-compose exec airflow-webserver bash -c "
  cd /opt/airflow/dbt_models && \
  dbt deps --profiles-dir . && \
  dbt run --profiles-dir . && \
  dbt test --profiles-dir .
"
```

### 6. Run Tests

```bash
# Install test deps locally
pip install -r requirements.txt

# Run all unit tests
pytest tests/unit/ -v

# With coverage report
pytest tests/ --cov=. --cov-report=html
```

---

## 📂 Project Structure

```
ecommerce-data-platform/
│
├── ingestion/                 # Data generation + Kafka producer
│   ├── schemas.py             # Pydantic entity schemas (User, Product, Order, Event)
│   ├── data_generator.py      # Fake data generator (weighted distributions)
│   └── kafka_producer.py      # Idempotent Kafka producer with retries
│
├── streaming/                 # Spark Structured Streaming
│   ├── spark_streaming.py     # Multi-topic Kafka consumer → bronze layer
│   └── stream_validator.py    # Data validation rules per entity
│
├── batch/                     # PySpark batch jobs
│   ├── spark_session.py       # Singleton SparkSession factory (S3A + Kafka)
│   ├── silver_processor.py    # Bronze → Silver (dedup, clean, type cast)
│   └── gold_aggregator.py     # Silver → Gold (business metrics)
│
├── warehouse/                 # PostgreSQL data warehouse
│   ├── models.py              # SQLAlchemy star schema models
│   └── postgres_loader.py     # Upsert loader with surrogate key resolution
│
├── dbt_models/                # dbt transformation layer
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/           # Views over raw warehouse tables
│       └── marts/             # Incremental business intelligence models
│
├── api/                       # FastAPI REST serving layer
│   ├── main.py                # App entrypoint with lifespan + middleware
│   ├── dependencies.py        # Async DB pool + dependency injection
│   ├── models.py              # Pydantic response schemas
│   └── routers/
│       ├── products.py        # /api/v1/products/top, /categories
│       ├── users.py           # /api/v1/users/activity, /{user_id}
│       └── metrics.py         # /api/v1/metrics/summary, /revenue/*, /categories
│
├── dashboard/                 # Streamlit analytics dashboard
│   └── app.py                 # Revenue trends, top products, user activity
│
├── airflow/
│   └── dags/
│       ├── batch_pipeline_dag.py   # Daily: silver → gold → warehouse
│       └── dbt_transform_dag.py    # Daily: dbt staging + marts + tests
│
├── docker/                    # Dockerfiles per service
│   ├── Dockerfile.ingestion
│   ├── Dockerfile.streaming
│   ├── Dockerfile.api
│   ├── Dockerfile.dashboard
│   └── init-postgres.sh       # Multi-DB PostgreSQL init
│
├── configs/                   # Centralized configuration
│   ├── kafka_config.py        # Kafka broker, topics, producer/consumer settings
│   ├── spark_config.py        # SparkSession, S3A, streaming settings
│   └── logging_config.py      # Rotating file + colored console logger
│
├── tests/
│   └── unit/
│       ├── test_data_generator.py  # Schema + generator tests
│       ├── test_api.py             # FastAPI endpoint tests (mocked DB)
│       └── test_validators.py      # PySpark validation tests
│
├── .env.example               # Environment variables template
├── docker-compose.yml         # Full stack compose (13 services)
├── requirements.txt           # All Python dependencies
└── pytest.ini                 # Test configuration
```

---

## 🔧 Production-Ready Features

| Feature | Implementation |
|---------|---------------|
| **Logging** | `configs/logging_config.py` — rotating file + colored console |
| **Retries** | `tenacity` with exponential backoff (Kafka, PostgreSQL) |
| **Data Validation** | `streaming/stream_validator.py` + Pydantic schema validation |
| **Schema Enforcement** | Spark StructType + Pydantic field validators |
| **Environment Config** | `.env.example` + `os.getenv()` throughout |
| **Idempotency** | Kafka idempotent producer + PostgreSQL `ON CONFLICT DO NOTHING/UPDATE` |
| **Incremental Processing** | Date-partitioned bronze/silver/gold + dbt incremental models |
| **Late Data Handling** | Watermarking in Spark Streaming + order_time offset simulation |
| **Partitioning** | Parquet partitioned by year/month/day/hour |
| **Query Optimization** | Adaptive query execution, partition pruning, indexed keys |

---

## 🧪 Testing Strategy

```bash
pytest tests/unit/test_data_generator.py  # 15 tests — schema + generator
pytest tests/unit/test_api.py             # 12 tests — FastAPI endpoints
pytest tests/unit/test_validators.py      # 10 tests — PySpark validation
```

---

## 📈 Kafka Topics

| Topic | Producer | Consumer | Partitions |
|-------|----------|----------|-----------|
| `user-events` | ingestion | spark-streaming | 3 |
| `orders` | ingestion | spark-streaming | 3 |
| `products` | ingestion | spark-streaming | 3 |
| `users` | ingestion | spark-streaming | 3 |

---

## 🛑 Stopping the Platform

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (full reset)
docker-compose down -v
```
