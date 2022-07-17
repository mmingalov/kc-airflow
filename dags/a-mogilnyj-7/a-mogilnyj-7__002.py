"""
DAG_Урок_4
Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds
"""

from airflow import DAG
import logging
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

DEFAULT_ARGS = {
    'start_date': days_ago(40),
    'owner': 'a-mogilnyj-7',
    'poke_interval': 600
}

with DAG("a-mogilnyj-7__002",
    schedule_interval='0 7 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-mogilnyj-7__002']
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    def get_article_func(article_id):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("article_cursor")
        logging.info("extract heading FROM articles")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {article_id}')
        one_string = cursor.fetchone()[0]
        return one_string

    get_article = PythonOperator(
        task_id='get_article',
        python_callable=get_article_func,
        op_args=['{{ dag_run.logical_date.weekday() + 1 }}'],
        do_xcom_push=True,
        dag=dag
    )

start >> get_article >> end
