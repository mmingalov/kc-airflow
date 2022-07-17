
import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a.petuhov_1',
    'poke_interval': 600
}

with DAG('a.petuhov_1_second_dag',
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a.petuhov_1']
          ) as dag:

    def current_exec_day():
        exec_day = datetime.datetime.today().weekday() + 1
        return exec_day

    getting_exec_day_num = PythonOperator(
        task_id='getting_exec_day_num',
        python_callable=current_exec_day
    )

    def GP_conn(**kwargs):
        ti = kwargs['ti']
        exec_day = ti.xcom_pull(task_ids='getting_exec_day_num')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor('named_cursor_name')
        cursor.execute(f'SELECT heading FROM articles WHERE id = {exec_day};')
        query_res = cursor.fetchall()
        result = ti.xcom_push(value=query_res, key='article')


    get_data = PythonOperator(
    task_id='get_data',
    python_callable=GP_conn,
    )


    getting_exec_day_num >> get_data




