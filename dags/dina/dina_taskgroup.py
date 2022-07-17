"""
Пример работы TaskGroup
"""

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

DEFAULT_ARGS = {
    'start_date': days_ago(12),
    'owner': 'Karpov',
    'poke_interval': 600
}

with DAG('dina_taskgroup',
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['karpov']
         ) as dag:

    start = DummyOperator(task_id='start')

    with TaskGroup(group_id='group1') as tg1:
        for i in range(5):
            DummyOperator(task_id=f'task{i + 1}')

    dummy = DummyOperator(task_id='dummy')

    with TaskGroup(group_id='group2') as tg2:
        for i in range(5):
            DummyOperator(task_id=f'task{i + 1}')

    end = DummyOperator(task_id='end')

    start >> tg1 >> dummy >> tg2 >> end
