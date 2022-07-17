"""
Урок 4. Домашнее задание
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm.alieva-2'
}

with DAG('m.alieva-2_dag_2',
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['m.alieva-2']
         ) as dag:

    start = DummyOperator(task_id='start')
    
    def load_heading_to_xcom_func(ds, **kwargs):
        exec_day = datetime.strptime(ds, '%Y-%m-%d')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor('named_cursor_name')
        cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(exec_day.weekday() + 1))
        query_res = cursor.fetchall()
        kwargs['ti'].xcom_push(value=query_res, key='heading')

    greenplum_conn = PythonOperator(
        task_id='greenplum_conn',
        python_callable=load_heading_to_xcom_func,
        provide_context=True
    )

    end = DummyOperator(task_id='end')

    start >> greenplum_conn >> end