"""
Первый DAG
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v.romanov-2',
    'poke_interval': 600
}


with DAG("v.romanov-2_lesson3_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['v.romanov-2']
         ) as dag:

    start = DummyOperator(task_id='start')

    def log_something_fun():
        logging.info('Hello, World')

    log_something = PythonOperator(
        task_id = 'log_something',
        python_callable = log_something_fun
    )

    start >> log_something

