"""
Даг-2
"""

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta, datetime
import logging

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'v-efimov-7',
    'provide_context': True,
    'depends_on_past': False,
    'start_date': datetime(2022, 4, 15, 6, 00),
    'email': ['victorefimov3@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='v-efimov-3-2',
    default_args=default_args,
    description='DAG-2',
    schedule_interval='@daily',
    tags=['v-efimov-7'],
) as dag:

    start_task = DummyOperator(task_id='start')

    def log_func(**kwargs):
        logging.info(f'op_args, {{ ds }}: ' + kwargs['ds'])

    second_task = PythonOperator(
    task_id='second_task',
    python_callable=log_func,
    provide_context=True
    )

    end_task = BashOperator(
        task_id='end_task',
        bash_command='echo {{ execution_date }}'
    )

    start_task >> second_task >> end_task


