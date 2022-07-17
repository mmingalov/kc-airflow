"""
Get data by week day
"""

from datetime import datetime
from airflow import DAG
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'e-nikitin-7',
    'poke_interval': 600
}

weekday = "{{ execution_date.strftime('%w') }}"

with DAG("e-nikitin-7-hw2",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['e-nikitin-7']
          ) as dag:

    dummy = DummyOperator(task_id='dummy')

    def log_greenplum(wd):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор

        cursor.execute(f'SELECT heading FROM articles WHERE id = {wd}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        logging.info(query_res)

    greenplum_processor = PythonOperator(
        task_id='greenplum_processor',
        python_callable=log_greenplum,
        op_args=[weekday],
        dag=dag
    )


    dummy >> greenplum_processor
