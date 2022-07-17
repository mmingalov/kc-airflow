"""
Это первый DAG, возможно что-то пойдёт не так :)
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import logging
import random

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

DEFAULT_ARGS = {
    'owner': 'g-sivash-4',
    'start_date': days_ago(1),
    'poke_interval': 600
}

dag = DAG(dag_id='g-sivash-4',
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1
          )

echo_ds = BashOperator(
    task_id='echo_ds',
    bash_command='echo {{ a-gajdabura }}',
    dag=dag
)


def print_hello_first():
    logging.info('Hello! Its task_1')


def print_hello_second():
    logging.info('Hello! Its Its task_2')


def get_random_num():
    return random.choice(['task_1', 'task_2'])


get_num = BranchPythonOperator(
    task_id='get_num',
    python_callable=get_random_num,
    dag=dag
)

task_1 = PythonOperator(
    task_id='task_1',
    python_callable=print_hello_first,
    dag=dag
)

task_2 = PythonOperator(
    task_id='task_2',
    python_callable=print_hello_second,
    dag=dag
)

echo_ds >> get_num >> [task_1, task_2]
