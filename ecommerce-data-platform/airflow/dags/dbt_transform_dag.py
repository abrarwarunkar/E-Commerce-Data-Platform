"""
Airflow DAG — runs dbt transformations after warehouse load.
Data Lineage: PostgreSQL -> dbt -> mart tables
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="ecommerce_dbt_transform",
    description="Runs dbt models after warehouse load completes",
    default_args=default_args,
    schedule_interval="0 5 * * *",   # 5AM UTC — after batch pipeline
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "dbt", "transform"],
) as dag:

    # Wait for upstream batch pipeline to complete
    wait_for_batch = ExternalTaskSensor(
        task_id="wait_for_batch_pipeline",
        external_dag_id="ecommerce_batch_pipeline",
        external_task_id="warehouse_load",
        timeout=3600,
        mode="reschedule",
        poke_interval=60,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt_models && dbt deps --profiles-dir .",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /opt/airflow/dbt_models && dbt run --select staging --profiles-dir .",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/airflow/dbt_models && dbt run --select marts --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_models && dbt test --profiles-dir .",
    )

    dbt_docs = BashOperator(
        task_id="dbt_generate_docs",
        bash_command="cd /opt/airflow/dbt_models && dbt docs generate --profiles-dir .",
    )

    wait_for_batch >> dbt_deps >> dbt_run_staging >> dbt_run_marts >> dbt_test >> dbt_docs
