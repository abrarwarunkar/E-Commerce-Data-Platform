"""
Airflow DAG — orchestrates the full batch pipeline.
Data Lineage: Airflow scheduler -> silver_processor -> gold_aggregator -> postgres_loader
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="ecommerce_batch_pipeline",
    description="Daily batch pipeline: bronze -> silver -> gold -> PostgreSQL",
    default_args=default_args,
    schedule_interval="0 3 * * *",   # 3AM UTC daily
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "batch", "silver", "gold"],
) as dag:

    def run_silver(**context):
        from datetime import date
        from batch.silver_processor import SilverProcessor
        process_date = date.fromisoformat(context["ds"])
        SilverProcessor().run(process_date)

    def run_gold(**context):
        from datetime import date
        from batch.gold_aggregator import GoldAggregator
        process_date = date.fromisoformat(context["ds"])
        GoldAggregator().run(process_date)

    def run_warehouse(**context):
        import os
        from datetime import date
        from warehouse.postgres_loader import PostgresLoader
        process_date = date.fromisoformat(context["ds"])
        silver_path = os.getenv("SILVER_PATH", "/data/silver")
        PostgresLoader().run(silver_path, process_date)

    silver_task = PythonOperator(
        task_id="silver_processing",
        python_callable=run_silver,
        doc_md="Cleans and deduplicates bronze data into silver layer.",
    )

    gold_task = PythonOperator(
        task_id="gold_aggregation",
        python_callable=run_gold,
        doc_md="Aggregates silver data into gold business metrics.",
    )

    warehouse_task = PythonOperator(
        task_id="warehouse_load",
        python_callable=run_warehouse,
        doc_md="Loads gold data into PostgreSQL star schema.",
    )

    silver_task >> gold_task >> warehouse_task
