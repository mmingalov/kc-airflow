"""
This is a test DAG for HomeWork!
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'start_date': days_ago(2),
    'owner': 'm-scherbinin-2',
    'poke_interval': 600
}

with DAG('marsel_dag',
         schedule_interval='@daily',
         default_args=default_args,
         max_active_runs=1,
         tags=['m-scherbinin']) as dag1:
    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag1
    )
    probe_dummy = DummyOperator(
        owner='m-scherbinin-2',
        task_id='probe_dummy',
        trigger_rule='one_success'
    )

echo_ds >> probe_dummy
