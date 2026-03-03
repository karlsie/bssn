from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.airflow_utils import load_api_to_postgres


DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="rest_api_to_postgres",
    default_args=DEFAULT_ARGS,
    description="Extract REST API data and load into PostgreSQL",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["api", "postgres", "etl"],
    params={
        "api_url": "http://localhost:8000/orders",
        "target_conn_id": "postgres_default",
        "target_table": "dwh.api_data",
    }
) as dag:

    api_load_to_pg_task = PythonOperator(
        task_id="load_api_to_postgres",
        python_callable=load_api_to_postgres,
    )


    api_load_to_pg_task
