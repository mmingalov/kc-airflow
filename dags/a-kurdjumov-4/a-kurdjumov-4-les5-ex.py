"""
Используем написанный operator для подсчета топ 3 локаций. 
Кривой запрос на инсерт исправил, exists тоже
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from a_kurdjumov_4_plugins.a_kurdjumov_4_ram_top_3 import KurdjumovRamTopNLocationsOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-kurdjumov-4',
    'poke_interval': 600
}

with DAG("a-kurdjumov-4-les5-ex",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-kurdjumov-4']
          ) as dag:
    

    start = DummyOperator(
        task_id='start'
    )

    get_top3_and_write = KurdjumovRamTopNLocationsOperator(
        task_id='get_top3_and_write',
        n=3
    )

    start>> get_top3_and_write
