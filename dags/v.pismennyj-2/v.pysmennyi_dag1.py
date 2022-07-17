import logging

from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(6),
    'owner': 'v.pysmennyi-1',
    'poke_interval': 600
}

with DAG(
    "pysmennyi_my_first_dag",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['v.pysmennyi-1']
    ) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    def my_first_dag():
        logging.info('my_first_dag')


    my_first_dag = PythonOperator(
        task_id='my_first_dag',
        python_callable=my_first_dag
    )

    start >> my_first_dag >> end
