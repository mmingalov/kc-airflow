"""
Домашнее задание урок 4
"""
import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'e-krupkina-3',
    'poke_interval': 600
}

with DAG("e_krupkina-3.hw2",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['e-krupkina-3']
          ) as dag:

    dummy_start = DummyOperator(task_id='start')

    dummy_end = DummyOperator(task_id='end')

    def connect_extract_func(*args, **kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        weekday = datetime.datetime.today().weekday() + 1

        cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(weekday))
        result = cursor.fetchall()

        logging.info(f'Fetched value: {result}')
        kwargs['ti'].xcom_push(value=result, key='article')

    connect_extract = PythonOperator(
        task_id='connect_to_greenplum_extract',
        python_callable=connect_extract_func,
        provide_context=True
    )

    dummy_start >> connect_extract >> dummy_end
