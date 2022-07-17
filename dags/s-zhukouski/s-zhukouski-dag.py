"""
Load heading from articles and export to XCom
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.operators.python import get_current_context
import logging


from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor


DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 's-zhukouski',
    'poke_interval': 600
}

dag = DAG("s-zhukouski_dag",
          schedule_interval='0 0 * * 1-6', #Run dag at 0:00 every day except Sunday
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['s-zhukouski']
          )

def get_day_week(**kwargs):
    context = get_current_context()
    exec_date = context["execution_date"]
    day_week = exec_date.weekday()+1
    logging.info(str(day_week))
    kwargs['ti'].xcom_push(value=day_week, key='day_week') #Sending the day of the week to run in the xcom parameter

explicit_push_day_week_task = PythonOperator(
    task_id='explicit_push_day_week',
    python_callable=get_day_week,
    provide_context=True,
    dag=dag
)

def load_data_from_greenplum(**kwargs):
    day_week=kwargs['ti'].xcom_pull(task_ids='explicit_push_day_week', key='day_week') #Getting the day of the week to run in the xcom parameter
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum') #Initialize the hook
    conn = pg_hook.get_conn() #Take a connection from it
    cursor = conn.cursor("named_cursor_name")
    logging.info(str(day_week))
    if day_week<7 :
        cursor.execute('SELECT heading FROM articles WHERE id = '+str(day_week)) #Execute sql with paramert
        logging.info('SELECT heading FROM articles WHERE id = '+str(day_week))
        one_string = cursor.fetchone()[0] #Returned a single value
        kwargs['ti'].xcom_push(value=one_string, key='value') #Sending result query in the xcom
        logging.info(one_string)


load_data_from_greenplum_task = PythonOperator(
    task_id='load_data_from_greenplum_task',
    python_callable=load_data_from_greenplum,
    dag=dag
)

explicit_push_day_week_task.set_downstream(load_data_from_greenplum_task)