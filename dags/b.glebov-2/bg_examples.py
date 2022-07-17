"""
HomeWork 1,2
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import datetime
import logging

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator


DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'b.glebov-2',
    'poke_interval': 600
}


dag = DAG("bg_second_dag",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['karpov', 'glebov', 'workdays', 'greenplum', 'XCom']
         )

start = DummyOperator(
    task_id='start_task',
    dag=dag,
  )

def greenplumFunc(**context):
    execution_date = context['execution_date']
    weekday = execution_date.weekday() + 1
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor("named_cursor_name")
    cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(weekday))
    query_res = cursor.fetchone()[0]
    logging.info("Got it")
    context['ti'].xcom_push(value=query_res, key='article')

greenplumTask = PythonOperator(
    task_id='greenplumTask',
    provide_context=True,
    python_callable=greenplumFunc,
    dag=dag
)

end = DummyOperator(
    task_id='end_task',
    dag=dag,
  )

start >> greenplumTask >> end

dag.doc_md = __doc__

start.doc_md = """dummy start"""
end.doc_md = """dummy end"""
