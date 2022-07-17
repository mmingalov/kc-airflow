"""
This is the second test, only the test
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import datetime
import logging

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime


DEFAULT_ARGS = {
    'start_date': datetime(2021, 11, 8),
    'end_date': datetime(2021, 12, 30),
    'owner': 'donskih',
    'poke_interval': 600
}

dag = DAG("a.donskih-2_second_dag",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['karpov']
          )

dummy_start = DummyOperator(
    task_id='start_task',
    dag=dag,
  )

dummy_end = DummyOperator(
    task_id='end_task',
    dag=dag,
  )


def green(context):
    execution_date = context['execution_date']
    weekday = execution_date.weekday() + 1
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor("named_cursor_name")
    cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(weekday))
    query_res = cursor.fetchone()[0]
    logging.info("Done")
    context['ti'].xcom_push(value=query_res, key='article')

green_task = PythonOperator(
    task_id='green_task',
    provide_context = True,
    python_callable=green,
    dag=dag
)



dummy_start >> green_task >> dummy_end
