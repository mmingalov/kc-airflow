"""
Задание часть 2
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(7),
    'owner': 'a-dobrin-3',
    'poke_interval': 600
}

with DAG("a-dobrin-3_hw",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['dobrin']
         ) as dag:

    def read_gp_to_xcom_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        exec_date_to_id = datetime.strptime(kwargs.get('a-gajdabura'), '%Y-%m-%d').weekday() + 1
        query = 'SELECT heading FROM articles WHERE id = {id_}'.format(id_=exec_date_to_id)
        logging.info(query)
        cursor.execute(query)
        one_string = cursor.fetchone()[0]
        logging.info('PUT in XCOM: {one_string_}'.format(one_string_=one_string))
        kwargs['ti'].xcom_push(value=one_string, key='return_val_gp')


    dummy = DummyOperator(task_id='dummy_start')

    read_gp_to_xcom_func = PythonOperator(
        task_id='read_gp_to_xcom',
        python_callable=read_gp_to_xcom_func,
        dag=dag
    )

    dummy >> read_gp_to_xcom_func
