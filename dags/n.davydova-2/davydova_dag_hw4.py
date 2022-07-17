"""
Простой тренировочный даг.

Cостоит из баш-оператора (выводит дату выполнения на стандартный вывод),
питон-оператора (выводит дату выполнения задачи в логи),
еще одного питон-оператора, забирающего из таблицы articles значение поля heading c id, равным дню недели запуска,
складывая получившееся значение в XCom.
"""

import datetime as dt
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2


DEFAULT_ARGS = {
    'start_date': dt.datetime(2021, 11, 14),
    'owner': 'NDavydova',
    'poke_interval': 600,
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['nitgrd2@ya.ru', ]
}

with DAG(
        "ndavydova_dag",
        schedule_interval='0 0 * * MON-SAT',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['karpov_courses'],
) as dag:
    
    print_ds = BashOperator(
        task_id='print_ds',
        bash_command='echo {{ a-gajdabura }}'
    )
    
    def first_func():
        logging.info(f'First task started at {dt.now()}')

    first_task = PythonOperator(
        task_id='first_task',
        python_callable=first_func
    )

    def get_data_articles_from_db(**kwargs):
        logging.info('Started')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        logging.info('Connected to DB')
        id_day = kwargs['execution_date'].isoweekday()
        logging.info(f'Executing for id_day = {id_day}')
        cursor.execute(f'SELECT heading FROM articles WHERE id = {id_day};')
        query_res = cursor.fetchone()             # get just one row
        logging.info(f': query_res = {query_res}')
        kwargs['t_i'].xcom_push(key='heading', value=query_res[0])
        logging.info('Finished')

    task_get_heading_from_articles = PythonOperator(
        task_id='get_heading_from_articles',
        python_callable=get_data_articles_from_db,
        provide_context=True
    )


print_ds >> first_task >> task_get_heading_from_articles

dag.doc_md = __doc__
print_ds.doc_md = """Выводит на стандартный вывод дату выполнения"""
first_task.doc_md = """Пишет в лог информацию о старте таски first_task"""
task_get_heading_from_articles = """Забирает heading из таблицы БД по id дня недели, кладет в XCom."""