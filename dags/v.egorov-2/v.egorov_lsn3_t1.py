#!coding: utf-8

from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import logging

from airflow import DAG

def log_start(**kwargs):
    kwargs['ti'].xcom_push(key='start_time', value=datetime.now())

def show_time(**kwargs):
    sd = str(kwargs['ti'].xcom_pull(key='start_time', task_ids='log_start'))
    logging.info('--------------------------------')
    logging.info('loged by task: ' + sd)
    logging.info('buit in: {{ts}}' + kwargs['ts'])
    logging.info('--------------------------------')


dag = DAG(
    'v.egorov_lsn3_t1',
    description='just_curious',
    schedule_interval='@once',
    start_date=datetime(2021, 11, 7),
    max_active_runs=1,
    catchup=False,
    tags=['egorov']
)

log_start_task = PythonOperator(task_id='log_start', python_callable=log_start, provide_context=True, dag=dag)

some_moves = DummyOperator(task_id='some_moves', dag=dag)

show_time_task = PythonOperator(task_id='show_time', python_callable=show_time, provide_context=True, dag=dag)

log_start_task >>  some_moves >> show_time_task