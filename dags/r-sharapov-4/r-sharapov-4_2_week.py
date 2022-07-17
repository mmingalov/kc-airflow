"""
test dag
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-gajdabura',
    'poke_interval': 600
}

with DAG("ds_test", schedule_interval='@daily', 
    default_args=DEFAULT_ARGS, max_active_runs=1, tags=['a-gajdabura']) as dag:
    dummy = DummyOperator(task_id="dummy")



    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ a-gajdabura }}',
        dag=dag
    )


    def hello_world_func():
        logging.info("hello_world")


    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )



    dummy >> [echo_ds, hello_world]
