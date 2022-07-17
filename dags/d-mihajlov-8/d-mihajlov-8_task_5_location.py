"""
location operator
"""
import random

from airflow import DAG
import logging
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from d_mihajlov_8_plugins.d_mihajlov_8_loc_operator import LocationOperator

FILE_NAME = 'top_location'
CSV_PATH = f'/tmp/{FILE_NAME}.csv'


def load_csv_to_gp_func():
    pg = PostgresHook('conn_greenplum')

    # insert new data
    pg.copy_expert(f"COPY d_mihajlov_8_ram_location FROM STDIN DELIMITER ','", CSV_PATH)

def table_exists_func():
    pg = PostgresHook('conn_greenplum_write')

    sql_create = f"""
            CREATE TABLE IF NOT EXISTS public.d_mihajlov_8_ram_location (
                id int4 NULL,
                "name" varchar(50) NULL,
                dimension varchar(50) NULL,
                resident_cnt int4 NULL,
                UNIQUE(id, "name", dimension, resident_cnt)
            )
            DISTRIBUTED BY (id);
            
            TRUNCATE public.d_mihajlov_8_ram_location;
                    """

    logging.info(f'SQL_create: \n{sql_create}')
    # del old data
    with pg.get_conn() as conn:
        cursor = conn.cursor()
    cursor.execute(sql_create)

DEFAULT_ARGS = {
    'start_date': datetime(2022, 5, 17),
    'end_date': datetime(2022, 5, 18),
    'owner': 'd-mihajlov-8',
    'depends_on_past': True
}

with DAG(
        dag_id='d-mihajlov-8_task_5_location',
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['shek']
) as dag:
    start = DummyOperator(
        task_id='start'
    )

    get_api_location = LocationOperator(
        task_id='get_location_df',
        top_location_count=3,
        CSV_PATH=CSV_PATH
    )

    is_table_exists = PythonOperator(
        task_id='check_table_exists',
        python_callable=table_exists_func
    )

    load_df_to_gp = PythonOperator(
        task_id='load_df_to_gp',
        python_callable=load_csv_to_gp_func
    )

    del_raw_files = BashOperator(
        task_id='del_raw_files',
        bash_command=f'rm {CSV_PATH}'
    )

    start >> get_api_location >> is_table_exists >> load_df_to_gp >> del_raw_files
