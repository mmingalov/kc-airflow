import numpy as np
import pandas as pd
import logging

from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from an_shishkov_plugins.ram_location_operator import anshishkovRamLocationsTopThreeOperator


default_args = {
    'owner': 'an-shishkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 5, 1)
}

create_table_statement = """
create table if not exists anshishkov_ram_location
(
    id int,
    name varchar,
    type varchar,
    dimension varchar,
    resident_cnt int
)
"""
truncate_table_statement = '''truncate anshishkov_ram_location'''

def check_or_create_table_func():
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(create_table_statement)
    cursor.execute(truncate_table_statement)
    conn.commit()
    conn.close()


def load_data_func(ti):
    df = ti.xcom_pull(key='return_value', task_ids='get_top_three_rams_locations_info')
    logging.info(df)
    
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    top_three_locations = list(df.itertuples(index=False, name=None))
    pg_hook.insert_rows(table='anshishkov_ram_location', rows=top_three_locations)


with DAG(
    dag_id='an-shishkov_ram_location_dag',
    schedule_interval='1 10 * * *',
    default_args=default_args,
    max_active_runs=1,
    tags=['an-shishkov']
) as dag:
    
    start = DummyOperator(task_id='start_dag')
    
    check_or_create_table = PythonOperator(
        task_id='check_or_create_table',
        python_callable=check_or_create_table_func
    )
    
    get_data = anshishkovRamLocationsTopThreeOperator(
        task_id='get_top_three_rams_locations_info'
    )
    
    load_data = PythonOperator(
        task_id='load_data_to_greenplum',
        python_callable=load_data_func
    )
    
    start >> check_or_create_table >> get_data >> load_data
