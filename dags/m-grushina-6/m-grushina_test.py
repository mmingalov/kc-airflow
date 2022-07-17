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
    'owner': 'm-grushina',
    'poke_interval': 600
}

with DAG("m-grushina_test",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['m-grushina']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
        )


    def hello_world_fnc():
        logging.info("hello, world!")


    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_fnc,
        dag=dag
    )
    dummy >>[echo_ds,hello_world]

