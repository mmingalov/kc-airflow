import datetime
import logging
import requests

from airflow import DAG
from airflow.utils.dates import days_ago
from d_potapov_plugins.d_potapov_resident_operator import dPotapovResidentsOperator

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

DEFAULT_ARGS = {
    'start_date': days_ago(0),
    'owner': 'd-potapov'
}

with DAG("d-potapov_practice_l5",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['d-potapov']
) as dag:
    start = DummyOperator(task_id="start")

    create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id='conn_greenplum_write',
        sql = f'''
        CREATE TABLE IF NOT EXISTS d_potapov_ram_location (
            id integer primary key,
            name text,
            type text,
            dimension text,
            resident_cnt integer
        )
        ''')

    get_top3_result = dPotapovResidentsOperator(
        task_id='get_top3_result'
    )

    start >> create_table >> get_top3_result