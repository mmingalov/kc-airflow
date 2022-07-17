from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 's.kitaev-2',
    'poke_interval': 600
}

with DAG("s.kitaev-2_first_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['s.kitaev-2']
         ) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    def hello_world():
        logging.info('Hello, World!')

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world
    )


    start >> [hello_world] >> end
