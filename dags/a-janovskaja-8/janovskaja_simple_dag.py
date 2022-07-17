"""
Simple DAG.
Writes to log execution_date and says hello.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-janovskaja-8',
    'poke_interval': 600
}

with DAG("janovskaja_simple_dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-janovskaja-8']
          ) as dag:

    dummy = DummyOperator(task_id='dummy')

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def greet():
        logging.info("Hello World!")

    greet_task = PythonOperator(
        task_id='greet_task',
        python_callable=greet,
        dag=dag
    )

    dummy >> [echo_ds, greet_task]

    dag.doc_md = __doc__

    echo_ds.doc_md = """Writes to log execution_date"""
    greet_task.doc_md = """Writes to 'Hello world'"""
