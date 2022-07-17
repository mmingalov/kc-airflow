"""
DAG для загрузки данных из GreenPlum по расписанию пн-сб через taskflow
"""
import logging

from airflow import DAG
from datetime import datetime

from airflow.decorators import dag, task

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'poke_interval': 600,
    'owner': 'd-bokarev'
}

@dag(default_args=DEFAULT_ARGS,
     schedule_interval='0 0 * * 1-6',
     max_active_runs=1,
     tags=['d-bokarev'])
def d_bokarev_taskflow_gp():
    @task
    def get_greeenplum_data(**kwargs):
        logging.info("Start logging")
        logging.info("DS: " + kwargs['ds'])
        gp_hook = PostgresHook(postgres_conn_id="conn_greenplum")
        week_day = datetime.strptime(kwargs['ds'], '%Y-%m-%d').weekday()+1
        logging.info(f'SELECT heading FROM articles WHERE id = { week_day }')
        conn = gp_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f'SELECT heading FROM articles WHERE id = { week_day }')
        results = cursor.fetchall()
        return results
    @task
    def print_results(arg):
        logging.info(arg)

    print_results(get_greeenplum_data())

d_bokarev_taskflow_gp_dag = d_bokarev_taskflow_gp()