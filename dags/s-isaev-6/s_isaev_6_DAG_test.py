"""
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-isaev-6',
    'poke_interval': 600
}

with DAG("s_isaev_6_DAG_test",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s-isaev-6']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='s_isaev_6_Bash',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def hello_world_func():
        logging.info("Hello World!1")

    hello_world = PythonOperator(
        task_id='s_isaev_6_Py',
        python_callable=hello_world_func,
        dag=dag
    )

    dummy >> [echo_ds, hello_world]