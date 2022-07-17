'''
Gets heading from article where id equal to day of week.
Being executed from 1/3/2022 - 14/3/22 from Mondays to Saturdays.
'''

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

import logging
from datetime import datetime

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'a-janovskaja-8',
    'poke_interval': 600
}

with DAG("a-janovskaja-8_lesson_4",
         schedule_interval='0 0 * * 1-6',
         catchup=True,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-janovskaja-8']) as dag:

    def get_article_heading(day_of_week):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')
        query_res = cursor.fetchall()

        logging.info(query_res)

    get_article_heading = PythonOperator(
                            task_id='get_article_heading',
                            python_callable=get_article_heading,
                            op_args=['{{ logical_date.weekday() + 1}}'])

    get_article_heading