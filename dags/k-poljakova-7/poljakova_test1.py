"""
Тестовый даг
"""
from airflow import DAG
from dateutil import tz
from airflow.utils.dates import days_ago, datetime
import logging
import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(60),
    'owner': 'poljakova',
    'poke_interval': 600
}

with DAG("poljakova_test1",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['poljakova']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def python_method(ds, **kwargs):
        Variable.set('execution_date', kwargs['execution_date'])
        return

    doit = PythonOperator(
        task_id='doit',
        provide_context=True,
        python_callable=python_method,
        dag=dag)

    dummy >> [echo_ds, doit]