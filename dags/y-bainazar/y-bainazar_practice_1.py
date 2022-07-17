"""
Это простейший даг.
баш-оператора (выводит execution_date),
двух питон-операторов (выводят по строке в логи)
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'y-bainazar',
    'poke_interval': 600
}

with DAG("y-bainazar_practice_1",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['y-bainazar']
         ) as dag:
    dummy = DummyOperator(task_id='dummy')

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )


    def hello_world_func():
        logging.info("Hello world!")


    hello_world = PythonOperator(
        task_id='first_task',
        python_callable=hello_world_func,
        dag=dag
    )

    dummy >> [echo_ds, hello_world]
