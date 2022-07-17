"""Забирает из GP articles
значения поля heading из строки
с id , равным дню недели
execution_date (понедельник=1, вторник=2, ...).
Складываем получившееся значение в XCom"""

from airflow.utils.dates import days_ago
import logging
from datetime import datetime
import time


from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook


start_dt = datetime.now()

DEFAULT_ARGS = {
    'start_date': days_ago(10),
    'owner': 'a-kravchenko-3',
}


@dag(
    schedule_interval='15 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=['a-kravchenko-3']
    )
def a_kravchenko_3_taskflow():

    @task
    def extract_rows():
        logging.info(f'init DAG at {start_dt}')
        logging.info('connecting to GP')

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")

        logging.info('Connected')

        id_day = time.gmtime()[6]+1
        logging.info(f'today\'s day number is {id_day}')

        logging.info('Execute SQL-query')
        cursor.execute(f'SELECT heading FROM articles WHERE id = {id_day}')
        logging.info('Fetch all rows')

        return cursor.fetchall()

    @task
    def push_2_xcom(rows: list, **kwargs):
        logging.info('init push values to XCOM')
        kwargs['task_instance'].xcom_push(key='heading', value=rows)

        end_dt = datetime.now()
        logging.info(f'end DAG at {end_dt}')

        delta_time = end_dt - start_dt
        logging.info(f'DAG\'s execution time: {delta_time}')

    push_2_xcom(extract_rows())


taskflow_dag = a_kravchenko_3_taskflow()
