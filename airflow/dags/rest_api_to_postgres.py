from datetime import datetime, timedelta
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


API_URL = "https://api.example.com/v1/data"
TABLE_NAME = "public.api_data"


def fetch_api_data(**context):
    """
    Fetch data from REST API.
    Supports simple pagination pattern.
    Pushes dataframe JSON to XCom.
    """

    headers = {
        "Authorization": "Bearer YOUR_TOKEN",  # Ideally from Airflow Variable or Secret
        "Content-Type": "application/json",
    }

    all_rows = []
    page = 1

    while True:
        response = requests.get(
            API_URL,
            headers=headers,
            params={"page": page, "limit": 100},
            timeout=60,
        )

        response.raise_for_status()
        data = response.json()

        rows = data.get("results", [])
        if not rows:
            break

        all_rows.extend(rows)

        # Pagination exit condition (adjust for your API)
        if not data.get("next"):
            break

        page += 1

    df = pd.json_normalize(all_rows)

    context["ti"].xcom_push(key="api_df_json", value=df.to_json(orient="records"))


def load_to_postgres(**context):
    """
    Load XCom JSON into PostgreSQL using bulk insert.
    """

    df_json = context["ti"].xcom_pull(
        key="api_df_json",
        task_ids="fetch_api_task",
    )

    if not df_json:
        return

    df = pd.read_json(df_json)

    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    df.to_sql(
        TABLE_NAME.split(".")[-1],
        engine,
        schema=TABLE_NAME.split(".")[0],
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )


with DAG(
    dag_id="rest_api_to_postgres",
    default_args=DEFAULT_ARGS,
    description="Extract REST API data and load into PostgreSQL",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["api", "postgres", "etl"],
) as dag:

    fetch_api_task = PythonOperator(
        task_id="fetch_api_task",
        python_callable=fetch_api_data,
    )

    load_postgres_task = PythonOperator(
        task_id="load_postgres_task",
        python_callable=load_to_postgres,
    )

    fetch_api_task >> load_postgres_task
