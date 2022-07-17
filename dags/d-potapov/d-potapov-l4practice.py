"""
d-potapov
Практика урока 4
"""
from airflow import DAG
from airflow.utils.dates import days_ago

import datetime
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_ARGS = {
    'start_date': datetime.datetime(2022,3,1),
    'end_date': datetime.datetime(2022,3,14),
    'owner': 'd-potapov',
    'catchup': True
}

with DAG("d-potapov_practice_l4",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['d-potapov']
) as dag:

    dayofweek = DummyOperator(
        task_id = 'get_day_of_week'
    )

    # Делаем запрос в GreenPlum
    def make_request(**context):
        day_of_week = context['execution_date']
        day_of_week = day_of_week.weekday()+1

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor('d-potapov')  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат

    # Выводим результат в лог
        for result in query_res:
            logging.info(result)
    
    get_gp_results = PythonOperator(
        task_id = 'get_results_from_gp',
        python_callable = make_request
    )

dayofweek >> get_gp_results