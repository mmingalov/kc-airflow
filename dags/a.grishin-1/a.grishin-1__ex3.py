import datetime as dt
import logging
import time

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2

dag = DAG(
    dag_id='simple_hello_dag',
    start_date=days_ago(1),
    schedule_interval="0 0 * * MON-SAT",
    description='Simple hello DAG'
)

task_hello_user = BashOperator(
    task_id='hello_user',
    bash_command='echo \"hello $USER\"',
    dag=dag
)

def print_arguments(arg1, arg2):
    logging.info('op_args: {} {}'.format(arg1, arg2))

task_print_arguments = PythonOperator(
    task_id='print_arguments',
    python_callable=print_arguments,
    op_args=['arg1', 'arg2'],
    dag=dag
)

def get_heading_from_articles(**kwargs):
    logging.info('Start PostgresHook')
    attempts = 5
    for i in range(attempts):
        try:
            pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
            conn = pg_hook.get_conn()
            cursor = conn.cursor("named_cursor_name")
            logging.info('Connected')
            id_day = kwargs['execution_date'].weekday() + 1
            logging.info(f'  DEBUG: id_day = {id_day}')
            cursor.execute(f'SELECT heading FROM articles WHERE id = {id_day};')
            query_res = cursor.fetchone()
            logging.info(f'  DEBUG: query_res = {query_res}')
            kwargs['task_instance'].xcom_push(key='heading', value=query_res[0])
            break
        except psycopg2.OperationalError as e:
            logging.error(f'ERROR: unable to connect. {e}')
            logging.error(f'ERROR: attempts {i} / {attempts}')
    print('Done PostgresHook')

task_get_heading_from_articles = PythonOperator(
    task_id='get_heading_from_articles',
    python_callable=get_heading_from_articles,
    dag=dag
)


task_hello_user >> task_print_arguments >> task_get_heading_from_articles
