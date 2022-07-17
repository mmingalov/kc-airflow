"""
Загружаем данные из API Рика и Морти
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import requests

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from m_grushina_6_plugins.m_grushina_6_top_operator import MGRRamDataLocationOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm-grushina',
    'poke_interval': 600
}


with DAG("m-grushina-6_load_ram",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['m-grushina']
         ) as dag:

    start = DummyOperator(task_id='start')

    print_data = MGRRamDataLocationOperator(
        task_id='print_data',
        arr_res=list()
    )



    start >> print_data
