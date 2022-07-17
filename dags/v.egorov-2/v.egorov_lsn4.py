#!coding: utf-8

from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import logging

from airflow import DAG

def getData(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor("named_cursor_name")
    cursor.execute('SELECT heading FROM articles WHERE id = {day}'.format(day=datetime.strptime(kwargs['a-gajdabura'], '%Y-%m-%d').weekday()+1))
    # logging.info(datetime.strptime(kwargs['a-gajdabura'], '%Y-%m-%d').weekday())
    # logging.info(kwargs['a-gajdabura'])
    query_res = cursor.fetchone()[0]
    kwargs['ti'].xcom_push(key='article_of_the_day', value=query_res)

dag = DAG(
    'v.egorov_lsn4',
    description='article_headings_from_Monday_to_Saturday',
    #schedule_interval='@once',
    schedule_interval='20 16 * * 1,2,3,4,5,6',
    start_date=datetime(2021, 11, 18),
    max_active_runs=1,
    catchup=False,
    tags=['egorov']
)

getData_task = PythonOperator(task_id='get_headings_from_green_plum', python_callable=getData, provide_context=True, dag=dag)
