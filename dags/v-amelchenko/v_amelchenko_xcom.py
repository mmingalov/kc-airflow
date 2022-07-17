"""
simple test dag
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

import datetime

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v-amelchenko',
    'poke_interval': 600
}

with DAG("v_amelchenko_xcom",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['va_xcom']
          ) as dag:

    begin_task_xcom = DummyOperator(task_id='begin_task_xcom')


    def push_date_to_xcom_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        weekday_int = datetime.datetime.today().isoweekday()

        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday_int}')
        cursor_res = cursor.fetchall()

        logging.info('--------------')
        logging.info(f'Weekday: {weekday_int}')
        logging.info(f'Result: {cursor_res}')
        logging.info('--------------')

        kwargs['ti'].xcom_push(value=cursor_res, key='cursor_res_xcom')


    push_date_to_xcom = PythonOperator(
        task_id='push_date_to_xcom',
        python_callable=push_date_to_xcom_func
    )

    begin_task_xcom >> push_date_to_xcom