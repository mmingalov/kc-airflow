"""
Начало более сложного дага
"""

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta, datetime
import logging

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'n_paveleva',
    'provide_context': True,
    'depends_on_past': False,
    'start_date': datetime(2022, 4, 3, 8, 15),
    'email': ['npvbox@mail.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='n-paveleva-7_dag2',
    default_args=default_args,
    description='More complex DAG',
    schedule_interval='@daily',
    tags=['n_paveleva'],
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


