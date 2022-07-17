"""
Простой учебный DAG
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-minjajlov',
    'poke_interval': 600
}

with DAG("a-minjajlov-lesson_01",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-minjajlov-lesson_01']
) as dag:

    dummy = DummyOperator(task_id='dummy')

    echo_ds = BashOperator(
        task_id='echo_ds',
        # bash_command='echo {{ a-minjajlov-lesson_01 }}',
        bash_command='echo abc',
        dag=dag
    )

    def simple_function():
        logging.info('Hello!!')

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=simple_function,
        dag=dag
    )

    dummy >> [echo_ds, hello_world]
