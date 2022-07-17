"""
Получает заголовки статей из таблицы articles за указанный день недели,
где id = день недели.
Выполняется с 1 марта 2022 по 14 марта 2022
"""
import logging

from airflow import DAG
import pendulum
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz="UTC"),
    'end_date': pendulum.datetime(2022, 3, 14, tz="UTC"),
    'owner': 'a-juferov',
    'poke_interval': 30
}


def get_articles_heading_func(day_of_week, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')
    query_res = cursor.fetchone()[0]
    logging.info(query_res)


with DAG("a-juferov_lesson4",
     schedule_interval='0 0 * * 1-6',
     default_args=DEFAULT_ARGS,
     max_active_runs=1,
     tags=['a-juferov']) as dag:

    get_articles_heading = PythonOperator(
        task_id='get_articles_heading',
        python_callable=get_articles_heading_func,
        op_args=['{{ logical_date.weekday() + 1 }}']
    )

    get_articles_heading


