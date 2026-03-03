from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime


def transfer_postgres_to_postgres(**context):
    # Runtime parameters (from manual trigger)
    conf = context["dag_run"].conf or {}

    source_conn_id = conf.get("source_conn_id", context["params"]["source_conn_id"])
    target_conn_id = conf.get("target_conn_id", context["params"]["target_conn_id"])
    source_table = conf.get("source_table", context["params"]["source_table"])
    target_table = conf.get("target_table", context["params"]["target_table"])

    source_hook = PostgresHook(postgres_conn_id=source_conn_id)
    target_hook = PostgresHook(postgres_conn_id=target_conn_id)

    source_conn = source_hook.get_conn()
    target_conn = target_hook.get_conn()

    source_cursor = source_conn.cursor()
    target_cursor = target_conn.cursor()

    try:
        source_cursor.execute(f"SELECT * FROM {source_table}")
        rows = source_cursor.fetchall()

        if rows:
            insert_query = f"""
                INSERT INTO {target_table}
                SELECT *
                FROM {source_table}
            """
            target_cursor.execute(insert_query)
            target_conn.commit()

    finally:
        source_cursor.close()
        target_cursor.close()
        source_conn.close()
        target_conn.close()


with DAG(
    dag_id="postgres_to_postgres_transfer",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["postgres", "etl"],
    params={  # Default values
        "source_conn_id": "bssn-dwh",
        "target_conn_id": "bssn-dwh",
        "source_table": "aset_tik",
        "target_table": "aset_tik_dt",
    },
) as dag:

    transfer_task = PythonOperator(
        task_id="transfer_data",
        python_callable=transfer_postgres_to_postgres,
    )

    transfer_task