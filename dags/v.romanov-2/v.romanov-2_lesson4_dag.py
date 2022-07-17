"""
DAG с ДЗ урока 4.
"""

import logging
import datetime
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v.romanov-2',
    'poke_interval': 600
}

with DAG("v.romanov-2_lesson4_dag",
         schedule_interval='0 0 * * MON-SAT',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['v.romanov-2']
         ) as dag:
    start = DummyOperator(task_id='start')


    def get_heading_from_gp_by_id(id):
        logging.info(f'selecting for id : {id}')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {id} ;')
        resp = cursor.fetchone()[0]
        logging.info(f'select result : {resp}')
        return resp

    def send_headline_to_XCom_func(**kwargs):
        today_as_int = datetime.datetime.today().isoweekday()
        heading = get_heading_from_gp_by_id(today_as_int)
        kwargs['ti'].xcom_push(value=heading, key='headline')

    send_headline_to_XCom = PythonOperator(
        task_id='send_headline_to_XCom',
        python_callable=send_headline_to_XCom_func,
        provide_context=True
    )

    def print_both_func(**kwargs):
        logging.info('--------------')
        logging.info(kwargs['ti'].xcom_pull(task_ids='send_headline_to_XCom', key='headline'))
        logging.info('--------------')

    print_both = PythonOperator(
        task_id='print_both',
        python_callable=print_both_func,
        provide_context=True
    )

    start >> send_headline_to_XCom >> print_both


