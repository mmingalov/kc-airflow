"""
simple test dag
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v-amelchenko',
    'poke_interval': 600
}

with DAG("v_amelchenko_test",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['vl_log']
          ) as dag:

    dummy_task_va = DummyOperator(task_id='dummy_task_va')

    bash_task_va = BashOperator(
        task_id='bash_task_va',
        bash_command='echo {{ ds }}'
    )

    def log_va_func():
        logging.info("Hello World!")

    hello_va = PythonOperator(
        task_id='hello_va',
        python_callable=log_va_func
    )

    dummy_task_va >> [hello_va, bash_task_va]