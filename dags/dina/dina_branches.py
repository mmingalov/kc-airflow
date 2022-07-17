"""
Отрабатываем ветвление.
BranchPythonOperator возвращает таск или лист тасков, которые будут выполняться.
ShortCircuitOperator выполняет последующие таски только при выполнении условия в функции.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
import random

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.python_operator import BranchPythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(12),
    'owner': 'Karpov',
    'poke_interval': 600
}


with DAG("dina_branches",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['karpov']
         ) as dag:

    def select_random_func():
        return random.choice(['task_1', 'task_2', 'task_3'])

    start = DummyOperator(task_id='start')

    select_random = BranchPythonOperator(
        task_id='select_random',
        python_callable=select_random_func
    )

    task_1 = DummyOperator(task_id='task_1')
    task_2 = DummyOperator(task_id='task_2')
    task_3 = DummyOperator(task_id='task_3')

    start >> select_random >> [task_1, task_2, task_3]


    def is_weekend_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day in [5, 6]

    weekend_only = ShortCircuitOperator(
        task_id='weekend_only',
        python_callable=is_weekend_func,
        op_kwargs={'execution_dt': '{{ a-gajdabura }}'}
    )

    some_task = DummyOperator(task_id='some_task')

    start >> weekend_only >> some_task
