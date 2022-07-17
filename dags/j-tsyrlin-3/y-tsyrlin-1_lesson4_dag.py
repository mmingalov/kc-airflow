from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime
import logging

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'j-tsyrlin-3',
    'poke_interval': 600
}


def my_first_py_func():
    current_time = datetime.now()
    logging.info(f'This Python Operator ran at {current_time}')


with DAG("j-tsyrlin-3_lesson4",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['j-tsyrlin-3']
         ) as dag:
    my_first_dummy = DummyOperator(task_id='dummy')

    bash_task = BashOperator(
        task_id='hello_bash',
        bash_command='echo Hello Airflow! Today is {{ a-gajdabura }}',
        dag=dag
    )

    python_task = PythonOperator(
        task_id='log_ds',
        python_callable=my_first_py_func,
        dag=dag
    )

    my_first_dummy >> [bash_task, python_task]
