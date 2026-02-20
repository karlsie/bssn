from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime


def transfer_postgres_to_postgres(**context):
    source_hook = PostgresHook(postgres_conn_id="postgres_source")
    target_hook = PostgresHook(postgres_conn_id="postgres_target")

    source_conn = source_hook.get_conn()
    target_conn = target_hook.get_conn()

    source_cursor = source_conn.cursor()
    target_cursor = target_conn.cursor()

    try:
        # Example: Read from source
        source_cursor.execute("""
            SELECT id, name, created_at
            FROM public.users
        """)
        rows = source_cursor.fetchall()

        if rows:
            # Example: Write to target
            insert_query = """
                INSERT INTO public.users_copy (id, name, created_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """

            target_cursor.executemany(insert_query, rows)
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
) as dag:

    transfer_task = PythonOperator(
        task_id="transfer_data",
        python_callable=transfer_postgres_to_postgres,
    )

    transfer_task
