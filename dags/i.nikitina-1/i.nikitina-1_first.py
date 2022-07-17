"""
Первый даг
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'i.nikitina-1',
    'poke_interval': 600
}

with DAG("i.nikitina-1_first",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['karpov']
         ) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    def hello_world_func():
        logging.info('Hello, World!')

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func
    )

    def bye_world_func():
        logging.info('Bye, World!')

    bye_world = PythonOperator(
        task_id='bye_world',
        python_callable=bye_world_func
    )

    start >> [hello_world, bye_world] >> end
