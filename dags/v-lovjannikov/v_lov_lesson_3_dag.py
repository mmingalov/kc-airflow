"""
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v-lovjannikov',
    'poke_interval': 600
}

with DAG("v_lov_lesson_3",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['v-lovjannikov']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_lovj = BashOperator(
        task_id='echo_lovj',
        bash_command='echo {{ "v-lo" }}',
        dag=dag
    )

    def hello_world_func():
        logging.info("Hello World!")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )

    dummy >> [echo_lovj, hello_world]
