"""
Получаем список всех коннекторов
"""

from airflow import DAG

import logging

from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.dates import days_ago
from airflow import settings
from airflow.models import Connection

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'poke_interval': 600,
    'owner': 'd-bokarev'
}

with DAG("d-bokarev_get_conn",
         default_args=DEFAULT_ARGS,
         schedule_interval='@once',
         tags=['d-bokarev']) as dag:
    def list_connections(**context):
        session = settings.Session()
        logging.info(session.query(BaseHook.get_connection('conn_greenplum_write')))

    list_conn = PythonOperator(
        task_id='list_connections',
        python_callable=list_connections,
        provide_context=True
    )
    list_conn