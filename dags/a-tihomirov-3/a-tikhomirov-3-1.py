"""
This is first DAG
Very simple
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'a-tikhomirov-3',
    'poke_interval': 600
}

def simple_logging_function():
    logging.info("Just writing to log...")

dag = DAG("a-tikhomirov-3-first-dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-tikhomirov-3']
          )

"""
Now operators
"""

start_task = DummyOperator(task_id='start_task', dag=dag)

end_task = DummyOperator(task_id='end_task', dag=dag)

python_task = PythonOperator(
    task_id='python_task',
    python_callable=simple_logging_function,
    dag=dag
)

bash_task = BashOperator(
    task_id='bash_task',
    bash_command='echo {{ ds_nodash }}',
    dag=dag
)

"""
Ok, all tasks together in DAG
"""

start_task >> [python_task, bash_task] >> end_task