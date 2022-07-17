"""
Домашнее задание №1
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'i.trubnikov',
    'poke_interval': 600
}

with DAG("i.trubnikov_test",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['i.trubnikov']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',

    )
    """
    создан специально чтобы всегда выбрасывать fail
    """
    echo_ds2_fail = BashOperator(
        task_id='echo_ds2',
        bash_command='echo {{ a-gajdabura }}',

    )

    def hello_world_func():
        logging.info("Hello World!")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,

    )

    dummy >> [echo_ds, hello_world, echo_ds2_fail]