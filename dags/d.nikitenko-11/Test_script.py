"""
Testing Airflow
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'dmnikitenko',
    'poke_interval': 600
}

with DAG("dmnikitnko_test",
         schedule_interval='40 11 * * *',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['dmnikitenko']
         ) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    def print_func():
        logging.info('Test success')

    print_task = PythonOperator(
        task_id='print_task',
        python_callable=print_func
    )

    start >> print_task >> end



