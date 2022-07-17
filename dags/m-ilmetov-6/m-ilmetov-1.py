"""
Забираем данные из CBR и складываем в Greenplum
"""

import logging
from airflow import DAG
from airflow.utils.dates import days_ago
import xml.etree.ElementTree as ET
import csv

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'start_date': days_ago(5) ,
    'owner': 'm-ilmetov-6',
    'wait_for_downstream': True,
}

dag_params = {
    'dag_id': 'm-ilmetov-6-1',
    'catchup': True,
    'default_args': default_args,
    'schedule_interval': '5 0 * * 1-6',
    'tags': ['m-ilmetov-6'],
}

with DAG(**dag_params) as dag:

    start = DummyOperator (
        task_id = 'start',
    )

    def _xcom_push_select(weekday) :
        pg_hook = PostgresHook ( postgres_conn_id = 'conn_greenplum' )
        conn = pg_hook.get_conn ( )
        cursor = conn.cursor ( "select_heading" )
        cursor.execute ( f'SELECT heading FROM articles WHERE id = {weekday}' )
        return cursor.fetchall ( )

    xcom_push_select = PythonOperator (
        task_id = 'xcom_push_select',
        python_callable = _xcom_push_select,
        op_args = [ '{{ data_interval_start.weekday() + 1 }}', ],
        do_xcom_push = True,
    )

    end = DummyOperator(
        task_id='end',
    )

    start >> xcom_push_select >> end
