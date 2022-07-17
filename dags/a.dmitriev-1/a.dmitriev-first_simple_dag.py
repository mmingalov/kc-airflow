"""
Это простейший даг.
Он состоит из сенсора (ждёт 6am),
баш-оператора (выводит execution_date),
двух питон-операторов (выводят по строке в логи)
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(12),
    'owner': 'a.dmitriev-1',
    'poke_interval': 600
}

with DAG("a.dmitriev-1_simple_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['karpov']
         ) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    def hello_world_func():
        print('Hello,  world!')

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func
    )

    def bye_world_func():
        print('Bye, world!')

    bye_world = PythonOperator(
        task_id='bye_world',
        python_callable=bye_world_func
    )

    start >> [hello_world, bye_world] >> end
