"""
4 урок
"""
from airflow import DAG
from airflow.utils.dates import datetime, days_ago
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 2),
    'end_date': datetime(2022, 3, 15),
    'owner': 'k-poljakova-7',
    'poke_interval': 600
}

dag = DAG("k-poljakova-7_4les",
    schedule_interval='0 0 * * *',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['k-poljakova-7'] )

def read_gp_func(article_id):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f'SELECT heading FROM articles WHERE id = {article_id};')
    return cursor.fetchone()[0]

read_gp = PythonOperator(
    task_id='read_gp',
    python_callable=read_gp_func,
    op_args=['{{ dag_run.logical_date.weekday()}}', ],
    do_xcom_push=True,
    dag=dag
)
