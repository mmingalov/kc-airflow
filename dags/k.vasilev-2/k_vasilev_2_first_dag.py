"""
Simple dag containing three tasks.
HW for the lesson #3, 4.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging


DEFAULT_ARGS = {
    'owner': 'k.vasilev-2',
    'email': ['start33kir@yandex.ru'],
    'start_date': days_ago(1),
}

with DAG(
    dag_id='k_vasilev_2_first_dag',
    schedule_interval="0 0 * * 1-6",
    default_args=DEFAULT_ARGS,
     max_active_runs=1,
    tags=['second_hw']
) as dag:

    start = DummyOperator(task_id='start')

    end = DummyOperator(
        task_id='end',
        trigger_rule='all_success')

    #def print_hello():
    #   logging.info("Hello!")

    #greatings = PythonOperator(
    #    task_id='test',
    #    python_callable=print_hello
    #)

    def get_heading(day_id: str):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute("SELECT heading FROM articles WHERE id = ?", day_id)
        return cursor.fetchone()[0]


    get_heading_op = PythonOperator(
        task_id='get_heading_articles',
        python_callable=get_heading,
        op_args=["{{ execution_date.weekday() }}"])


start >> get_heading_op >> end
