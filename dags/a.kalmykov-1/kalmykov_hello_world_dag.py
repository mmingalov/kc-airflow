import logging

from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(12),
    'owner': 'Karpov',
    'poke_interval': 600
}


def hello_world():
    logging.info('Hello world')


with DAG(
    "kalmykov_ad_hello_world",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['karpov']
    ) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world
    )

    start >> hello_world >> end
