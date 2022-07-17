"""
Учимся делать DAG
"""

from multiprocessing.dummy import DummyProcess
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-kurdjumov-4',
    'poke_interval': 600
}

with DAG("a.kurdyumov-1st_dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-kurdjumov-4']
          ) as dag:
    dummy = DummyOperator(task_id='dummy')
    

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ a-gajdabura }}',
        dag=dag
    )


    def first_func():
        logging.info("Hello world")


    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=first_func,
        dag=dag
    )


    dummy>> [echo_ds, hello_world]
