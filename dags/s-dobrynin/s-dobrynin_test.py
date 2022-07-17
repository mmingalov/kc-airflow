"""
dag test
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-dobrynin',
    'poke_interval': 600
}

with DAG("s_dobrynin_test",
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['s-dobrynin']
        ) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_s_dobrynin = BashOperator(
        task_id='echo_s_dobrynin',
        bash_command='echo {{ s-dobrynin }}',
        dag=dag
    )


    def hello_world_func():
        logging.info("Hello world")


    hello_world = PythonOperator(
        task_id='Hello world',
        python_callable=hello_world_func,
        dag=dag
    )

    dummy >> [echo_s_dobrynin, hello_world]