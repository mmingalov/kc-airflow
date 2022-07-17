"""
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import datetime


from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    'start_date': '01.03.2022',
    'end_date': '14.03.2022',
    'owner': 'a-knjazkov-7',
    'poke_interval': 600
}

with DAG(
        "a-knjazkov-7_heading",

        schedule_interval='0 0 * * 1-6',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['a-knjazkov-7_heading']
) as dag:

         dummy = DummyOperator(task_id="dummy")

         def day_week_func():
            wd = datetime.datetime.now().isoweekday()
            pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
            conn = pg_hook.get_conn()  # берём из него соединение
            cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
            cursor.execute(f'SELECT heading FROM articles WHERE id = {wd}')  # исполняем sql
            query_res = cursor.fetchall()
            logging.info(query_res)


        day_week = PythonOperator(
            task_id='day_week',
            python_callable=day_week_func,
            dag=dag
        )

        dummy >> day_week
