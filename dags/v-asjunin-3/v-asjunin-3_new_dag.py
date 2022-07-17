#
# """
# Это мой первый Даг
# """
#

import datetime
import logging

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(0),
    'owner': 'Slava',
    'poke_interval': 600
}

dag = DAG('v-asjunin-3_new_dag',
    default_args=default_args,
    schedule_interval='5 0 * * 1-6',
    max_active_runs=1,
    tags=['v-asjunin']
)

poehali = DummyOperator(task_id='Poehali', dag=dag)

def getDaysHeading(**kwargs):
    hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f'SELECT heading FROM articles WHERE id = {datetime.datetime.today().isoweekday()}')
    heading_string = cursor.fetchall()
    logging.info('--------------')
    logging.info(heading_string)
    logging.info('--------------')
    kwargs['ti'].xcom_push(value=heading_string, key='article')

goto_Nash_GreenPlum = PythonOperator(
    task_id='goto_Nash_GreenPlum',
    python_callable=getDaysHeading,
    provide_context=True,
    dag=dag
)

poehali >> goto_Nash_GreenPlum