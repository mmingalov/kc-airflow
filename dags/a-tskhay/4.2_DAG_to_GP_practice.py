from airflow import DAG
import logging

from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022,3,1),
    'end_date': datetime(2022,3,15),
    'owner': 'a-tskhay'
}

with DAG(
    'a-tskhay_extracting_from_GP',
     schedule_interval='0 8 * * 1-6',
     default_args=DEFAULT_ARGS,
     tags=['a-tskhay']
) as dag:

    query = 'SELECT heading FROM articles WHERE id = ({{ execution_date.weekday() }} + 1);'

    def extract_func(query):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(query)
        query_res = cursor.fetchall()
        # one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info('query is ' + query)
        logging.info('result is ' + str(query_res))

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_func,
        op_args=[query]
    )

extract_task