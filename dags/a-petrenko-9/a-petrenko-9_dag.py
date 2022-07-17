"""
DAG для получения данных из таблицы с фильтром по днем недели и вывод их в лог
"""

from airflow.decorators import dag, task
import logging

from datetime import datetime
import pendulum
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz="UTC"),
    'end_date': pendulum.datetime(2022, 3, 14, tz="UTC"),
    'owner': 'a-petrenko-9',
    'poke_interval': 600
}

@dag(
    dag_id="a-petrenko-9_simple_dag",
    start_date=pendulum.datetime(2022, 3, 1, tz="UTC"),
    end_date=pendulum.datetime(2022, 3, 14, tz="UTC"),
    schedule_interval='0 0 * * MON-SAT',
    catchup=True,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['ap-9']
)
def using_dag_flow():

    def get_articles_data_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        date = datetime.today()
        logging.info('DS date = ', date)
        #date.replace(' ', '-')
        #year, month, day, time = date.split('-')
        #day_of_week = datetime.date(int(year), int(month), int(day)).isoweekday()
        #cursor.execute('SELECT heading FROM articles WHERE id = '+day_of_week)
        #query_res = cursor.fetchall()
        #one_string = cursor.fetchone()[0]
        #logging.info('FETCH ALL = ', query_res)
        #logging.info('FETCH ONE STRING = ', one_string)

    get_articles_data = PythonOperator(
        task_id='get_articles_data',
        python_callable=get_articles_data_func
    )

    get_articles_data


dag = using_dag_flow()