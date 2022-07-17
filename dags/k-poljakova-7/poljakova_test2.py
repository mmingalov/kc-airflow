"""
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago, datetime
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 2),
    'end_date': datetime(2022, 3, 15),
    'owner': 'poljakova',
    'poke_interval': 600
}

dag = DAG("poljakova_test2",
    schedule_interval='0 0 * * 1-2',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['poljakova'] )


def read_gp_func(article_id):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f'SELECT heading FROM articles WHERE id = {article_id};')
    return cursor.fetchone()[0]


read_gp = PythonOperator(
    task_id='read_gp',
    python_callable=read_gp_func,
    op_args=['{{ dag_run.logical_date.weekday() + 1}}', ],
    do_xcom_push=True,
    dag=dag
)