from airflow.utils.dates import days_ago
import pandas as pd
from sqlalchemy import create_engine

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from mukhachev_plugins.Mukhachev_plugin import MukhachevOperator

TABLE_NAME = 'public.v_amukhachev_1_ram_location'
GREENPULM_CONN = 'conn_greenplum_write'

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a.muhachev',
    'poke_interval': 600
}


with DAG('Mukhachev_RAM',
         schedule_interval='0 0 * * *',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a.mukhachev']
         ) as dag:
    start = DummyOperator(task_id='start')

    create_table = PostgresOperator(
        task_id='create_table',
        sql=f'''
                    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    id INTEGER NOT NULL,
                    name VARCHAR NOT NULL,
                    type VARCHAR NOT NULL,
                    dimension VARCHAR NOT NULL,
                    resident_cnt INTEGER NOT NULL,
                    PRIMARY KEY (id)
                    );
                ''',
        postgres_conn_id=GREENPULM_CONN,
        dag=dag
    )
    delete_rows = PostgresOperator(
        task_id='delete_rows',
        postgres_conn_id=GREENPULM_CONN,
        sql=f"DELETE FROM {TABLE_NAME};",
        autocommit=True
    )

    load_ram_locations = MukhachevOperator(
        task_id='load_ram_locations',
        pages_count=3,
        gp_table_name=TABLE_NAME,
        gp_conn_id=GREENPULM_CONN
    )

    end = DummyOperator(task_id='end')

    start >> create_table >> delete_rows >> load_ram_locations >> end


