"""
Мой тестовый даг (i-oralin-7)
"""
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
# from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'i-oralin-7',
    'poke_interval': 600
}

with DAG("i-oralin-7-hw3-simple-dag",
         schedule_interval='@hourly',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['i-oralin-7']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ts = BashOperator(
        task_id='echo_ts',
        bash_command='echo Bash says: Current timestamp: {{ ts }}',
        # dag=dag
    )


    def python_op():
        logging.info("Hello, this is my first DAG")


    py_hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=python_op,
        # dag=dag
    )

    ls_bash = BashOperator(
        task_id='ls_bash',
        bash_command='echo Bash runs ls command: $(ls)',
    )

    dummy >> [echo_ts, py_hello_world] >> ls_bash
