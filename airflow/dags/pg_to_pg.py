from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.airflow_utils import transfer_postgres_to_postgres


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