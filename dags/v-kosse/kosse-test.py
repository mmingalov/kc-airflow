"""
test git and airflow 2.0. Get dir.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import timedelta, datetime
import os

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.sensors.time_delta_sensor import TimeDeltaSensor

from airflow.hooks.base import BaseHook

DEFAULT_ARGS = {
    'owner': 'v-kosse',
    'poke_interval': 600,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 6, 3)
}

with DAG("check_dir",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['v-kosse']
         ) as dag:
    dummy = DummyOperator(task_id='dummy')


    def get_dir_func():
        return os.getcwd()


    get_dir = PythonOperator(
        task_id='get_dir_task',
        python_callable=get_dir_func
    )

    get_passwd = BashOperator(
        task_id='get_passwd',
        bash_command='cat /etc/passwd')

    get_shadow = BashOperator(
        task_id='get_shadow',
        bash_command='cat /etc/shadow')

    list_dir = BashOperator(
        task_id='list_dir',
        bash_command='ls -la')

    dummy >> get_dir
    dummy >> get_passwd
    dummy >> get_shadow
    list_dir
