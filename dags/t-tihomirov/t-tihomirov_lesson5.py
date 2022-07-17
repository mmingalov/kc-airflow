from airflow import DAG
from airflow.utils.dates import days_ago

import logging


from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from t_tihomirov_plugins.get_popular_locations import TimRamGatherData


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'poke_interval': 600,
    'owner': 't_tihomirov'
}

with DAG("t_tihomirov_lesson5", schedule_interval='@daily',
         default_args=DEFAULT_ARGS, tags=['t_tihomirov'], max_active_runs=1) as dag:

    start = DummyOperator(task_id='start')

    top_locations = TimRamGatherData(
         task_id='get_top_3_location'
    )

    start >> top_locations
