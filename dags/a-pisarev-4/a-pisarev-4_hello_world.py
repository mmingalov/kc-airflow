"""
???????? ???
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-pisarev-4',
    'poke_interval': 600
}

with DAG("a-pisarev-4_test_dag",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-pisarev-4']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_apisarev4 = BashOperator(
        task_id='echo_apisarev4',
        bash_command='echo {{ a-gajdabura }}',
        dag=dag
    )

    def hello_world_func():
        logging.info("Hello World! a-pisarev-4 =)")

    hello_world_apisarev4 = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )

    dummy >> [echo_apisarev4, hello_world_apisarev4]