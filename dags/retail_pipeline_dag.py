from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id='retail_data_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    run_etl = BashOperator(
        task_id='run_retail_etl',
        bash_command='python /opt/airflow/dags/scripts/etl_job.py'
    )

    run_etl
