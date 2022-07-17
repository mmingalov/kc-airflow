"""
Складываем курс валют в GreenPlum (Меняем описание нашего дага)
"""

from airflow import DAG
from datetime import datetime
import logging

from airflow.hooks.postgres_hook import PostgresHook # c помощью этого hook будем входить в наш Greenplan
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 2),
    'end_date': datetime(2022, 3, 15),
    'owner': 'd.nigmatullin-9',
    'poke_interval': 600
}

with DAG("d.nigmatullin-9_db", # Меняем название нашего DAG
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['d.nigmatullin-9']
          ) as dag:

    start = DummyOperator(task_id="start")

    def taking_one_string_from_db_func(article_id):
        article_id = article_id.replace('"', '')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и именованный (необязательно) курсор
        logging.info(f'_________________ WHERE id = {article_id}')
        cursor.execute(f'SELECT heading FROM articles WHERE id = {article_id}')
        return cursor.fetchone()[0]


    taking_one_string_from_db = PythonOperator(
        task_id='taking_one_string_from_db',
        python_callable=taking_one_string_from_db_func,
        op_args=['{{ dag_run.logical_date.weekday() + 1 }}', ],
        do_xcom_push=True,
        dag=dag
    )

    start >> taking_one_string_from_db