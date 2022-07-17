"""
simple test dag
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

from v_amelchenko_plugins.v_amelchenko_ram_operator import VladRickMortyOperator


import datetime

TABLE_NAME = 'v_amelchenko_ram_top'


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v-amelchenko',
    'poke_interval': 600
}

with DAG("v_amelchenko_ram_top",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['va_ram_top']
          ) as dag:

    create_table_ram_top = PostgresOperator(
        task_id='create_table_ram_top',
        postgres_conn_id='conn_greenplum_write',
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            type VARCHAR NOT NULL,
            dimension VARCHAR NOT NULL,
            resident_cnt INTEGER NOT NULL);
             """,
        autocommit=True,
        )

    truncate_table = PostgresOperator(
        task_id='truncate_table',
        postgres_conn_id='conn_greenplum_write',
        sql=f"TRUNCATE TABLE {TABLE_NAME};",
        autocommit=True,
    )


    top_location_write = VladRickMortyOperator(
        task_id='top_location_write'
    )

    def print_top_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(f'SELECT * FROM {TABLE_NAME}')
        cursor_res = cursor.fetchall()

        logging.info('--------------')
        logging.info(f'Result: {cursor_res}')
        logging.info('--------------')


    print_top = PythonOperator(
        task_id='print_top',
        python_callable=print_top_func
    )

    create_table_ram_top >> truncate_table >> top_location_write >> print_top

