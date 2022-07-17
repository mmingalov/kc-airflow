"""
Даг ДЗ для 3-го урока
"""

import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'nvakhrushev',
    'poke_interval': 600
}

with DAG(
        dag_id='nvakhrushev_test',
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['karpov']
) as dag:

    bash_test = BashOperator(
        task_id='bash_test',
        bash_command='echo "Text with bash!"'
    )

    def print_hw():
       logging.info("Hello World!")

    python_test = PythonOperator(
        task_id='python_test',
        python_callable=print_hw
    )

    end = DummyOperator(
        task_id = 'end',
        trigger_rule='all_success'
    )

    bash_test >> python_test >> end