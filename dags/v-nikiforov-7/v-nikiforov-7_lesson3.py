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
    'owner': 'v-nikiforov-7',
    'poke_interval': 600
}

with DAG("v-nikiforov-7_lesson3",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['v-nikiforov-7']
    ) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def output_func():
        logging.info("Peace to the World!")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=output_func,
        dag=dag
    )

    dummy >> [echo_ds, hello_world]