"""
Тестовый даг
"""
import random

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(7),
    'owner': 'd-mihajlov-8',
}
tasks = []

with DAG(
        dag_id='d-mihajlov-8_test_dag',
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['shek']
) as dag:
    dummy = DummyOperator(
        task_id='start'
    )
    for task_id in range(1, 6):
        echo_ds = BashOperator(
            task_id=f'task_{task_id}',
            bash_command='echo {{ ds }}'
        )
        tasks.append(echo_ds)


    def select_random_func():
        return random.choice([f'task_{i}' for i in range(1, 6)])

    choose_task = BranchPythonOperator(
        task_id='choose_task',
        python_callable=select_random_func
    )

    dummy >> choose_task >> tasks
