"""
Задание из урока 3

Требования:
— DummyOperator
— BashOperator с выводом строки
— PythonOperator с выводом строки
— любая другая простая логика
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator

import datetime
import random

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-chertkov',
    'poke_interval': 600
}


with DAG("s-chertkov-task-3",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s-chertkov']
) as dag:

    """
    Просто dummy, не делает ничего
    """
    dummy = DummyOperator(task_id="dummy")

    """
    Показывает свободное место на диске воркера
    """
    disk_info_operator = BashOperator(
        task_id='s-chertkov-df',
        bash_command='df -lah'
    )

    def is_today_a_weekend():
        logging.info("Let's check if today is weekend!")
        if datetime.datetime.today().weekday() > 4:
            logging.info("Yes, it's weekend, let's party!")
        else:
            logging.info("Sadly no, go back to work!")

    is_weekend_operator = PythonOperator(
        task_id='s-chertkov-is-weekend',
        python_callable=is_today_a_weekend,
    )



    def select_random_func():
        return random.choice(['test_random_task_1', 'test_random_task_2'])

    select_random = BranchPythonOperator(
        task_id='select_random',
        python_callable=select_random_func
    )

    task_1 = DummyOperator(task_id='test_random_task_1')
    task_2 = DummyOperator(task_id='test_random_task_2')

    select_random >> [task_1, task_2]
    dummy >> [disk_info_operator, is_weekend_operator, select_random]
