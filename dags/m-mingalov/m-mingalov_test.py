"""
Test DAG
LESSON 3 task 1
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm-mingalov',
    'poke_interval': 600
}

with DAG(
    dag_id="m-mingalov_test",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['m-mingalov']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def hello_world_func():
        logging.info("Hello World! This is my first DAG")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )
    #dag.doc_md = __doc__
    #echo_ds.doc_md = """echo_ds doc"""
    #hello_world.doc_md = """hello_world doc"""

    dummy >> [echo_ds, hello_world]