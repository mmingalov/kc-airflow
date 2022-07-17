""" Идем в greenplum, забираем из таблицы articles значение поля heading из строки с id = дню недели и
выводим результат работы в виде логов"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging
from airflow.operators.python import get_current_context

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'owner': 's-tselikov',
    'poke_interval': 600,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    }
schedule_interval = '20 12 * * 1-6'

@dag(default_args = DEFAULT_ARGS,
     catchup=True,
     tags=['s-tselikov'],
     schedule_interval = schedule_interval)
def get_data_from_gp_heading():

    #получаем сегодняшний день недели (1 - понедельник, 2 - вторник...)
    @task()
    def day_now():
        context = get_current_context()
        ds = context["ds"]
        weekday = datetime.strptime(ds, '%Y-%m-%d').weekday() + 1
        return weekday

    #Получаем данные из GreenPlum за соответствующий день
    @task()
    def get_data_from_gp(daynow):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {daynow}')
        #db_result = cursor.fetchone()[0]
        query_res = cursor.fetchall()
        return query_res
    #выводим на печать в лог
    @task(do_xcom_push = True)
    def print_data_to_log(daynow, total):
        logging.info(str(daynow))
        logging.info(total)

    task1 = day_now()
    task2 = get_data_from_gp(task1)
    task3 = print_data_to_log(task1, task2)

s_tselikov_get_dag = get_data_from_gp_heading()