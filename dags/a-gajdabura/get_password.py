"""
Получаем пароль от GreenPlum
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging


DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a_gajdabura_plugins',
    'poke_interval': 600
}

with DAG("a-gajdabura_get_password",
          schedule_interval='@once',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a_gajdabura_plugins']
          ) as dag:

    dummy = DummyOperator(task_id='dummy')

    def get_password_func():
        logging.info(PostgresHook.get_connection('conn_greenplum_write').password)


    get_password = PythonOperator(
        task_id='get_password',
        python_callable=get_password_func,
)


