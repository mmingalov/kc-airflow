"""
This is the first homework for ETL by Alexander Parshin
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(7),
    'owner': 'a.parshin-2',
    'poke_interval': 600
}


with DAG("a.parshin-2_1",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a.parshin-2']
         ) as dag:

    # start = DummyOperator(task_id='start')

    def check_day(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day not in [6]

    check_day = ShortCircuitOperator(
        task_id='check_day',
        python_callable=check_day,
        op_kwargs={'execution_dt': '{{a-gajdabura}}'}
                   )


    def get_data(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        comm = f"SELECT heading FROM articles WHERE id = {exec_day+1}"

        pg_hook = PostgresHook('conn_greenplum')
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и именованный (необязательно) курсор
        cursor.execute(comm)  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        # one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info(query_res)
        return query_res

    get_data = PythonOperator(
        task_id='get_data',
        python_callable=get_data,
        op_kwargs={'execution_dt': '{{a-gajdabura}}'}
    )


    def print_query(**kwargs):
        logging.info('--------------')
        logging.info(kwargs['templates_dict']['implicit'])
        logging.info('--------------')


    print_query = PythonOperator(
        task_id='print_query',
        python_callable=print_query,
        templates_dict={'implicit': '{{ ti.xcom_pull(task_ids="get_data") }}'},
        provide_context=True
    )

    check_day >> get_data >> print_query