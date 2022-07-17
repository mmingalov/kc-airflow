"""самый первый даг"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
import logging
import random

from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(4),
    'owner': 'a-shumilov-4',
    'poke_interval': 600,
    'retries': 3,
    'retry_delay': 10,
    'priority_weight': 2,
    'end_date': datetime(2022, 2, 7)

}

with DAG("a-shumilov-4",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-shumilov-4_3']
          ) as dag:

    dummy = DummyOperator(task_id='start')

    def select_random_func():
        random.seed(datetime.now())
        return random.choice(['echo_task', 'Python_task'])

    select_random = BranchPythonOperator(
        task_id='select_random',
        python_callable=select_random_func
    )

    def ds_func(**kwargs):
        ds = kwargs['templates_dict']['date']
        logging.info(ds)

    Python_task = PythonOperator(
        task_id = 'Python_task',
        python_callable = ds_func,
        templates_dict = {'date': '{{ a-gajdabura }}'},
        dag = dag
    )

    echo_task = BashOperator(
        task_id = 'echo_task',
        bash_command = 'echo {{ a-gajdabura }}',
        dag = dag
    )


    dummy >> select_random >> [echo_task, Python_task]