"""
ДЗ Урок 5 (i-oralin-7)
"""
import logging

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
# => pip install apache-airflow-providers-postgres
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from airflow.utils.timezone import datetime

# IDE thinks it's error but it works fine and should be imported like this
from i_oralin_7_plugins.i_oralin_7_ram_plugins import IOralin7TopLocationsByResidentsOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 4, 7),
    'owner': 'i-oralin-7',
    'poke_interval': 600
}

with DAG("i-oralin-7-hw5-ram-locations",
         schedule_interval="@daily",
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['i-oralin-7'],
         # Do run for all previous days
         catchup=True
         ) as dag:
    dummy = DummyOperator(task_id="start")
    get_top3_locations = IOralin7TopLocationsByResidentsOperator(task_id='get_top3_locations')

    my_table_name = 'i_oralin_7_ram_location'

    create_ram_location_table = PostgresOperator(
        task_id="create_ram_location_table",
        postgres_conn_id='conn_greenplum_write',
        # TODO it is better to put SQL to separate file
        sql=f"""
            CREATE TABLE IF NOT EXISTS {my_table_name} (
            id int PRIMARY KEY,
            name text,
            type text,
            dimension text,
            resident_cnt int);
            -- TRUNCATE to clear existing data if any            
            TRUNCATE {my_table_name};
          """)

    #      -- TRUNCATE to clear existing data if any
    clean_ram_location_table = PostgresOperator(
        task_id="clean_ram_location_table",
        postgres_conn_id='conn_greenplum_write',
        sql=f"""
            TRUNCATE {my_table_name};
          """)

    def insert_top_to_gp_func(**kwargs):
        top_locations = kwargs['ti'].xcom_pull(task_ids='get_top3_locations', key='top_locations')
        logging.info(f'Got top locations from XCom: {top_locations}')
        target_fields=['id', 'name', 'type', 'dimension', 'resident_cnt']
        rows = [(loc['id'], loc['name'], loc['type'], loc['dimension'], len(loc['residents']))
            for loc in
            top_locations]
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.insert_rows(table=my_table_name,
                            rows=rows,
                            target_fields=target_fields,
                            # replace=True
                            )

    insert_top_to_gp = PythonOperator( task_id='insert_top_to_gp',
                                       python_callable=insert_top_to_gp_func,
                                       # it is deprecated: provide_context=True
                                       )
    dummy >> get_top3_locations >> create_ram_location_table >> clean_ram_location_table >> insert_top_to_gp

    # Documentation
    dag.doc_md = __doc__

    get_top3_locations.doc_md = """Gets top 3 rick and morty locations by resident count"""
    create_ram_location_table.doc_md = """Creates new RAM table if not exists"""
    clean_ram_location_table.doc_md = """Truncates existing RAM table"""
    insert_top_to_gp.doc_md = """Gets result of get_top3_locations and writes it to the table"""

# TODO try using taskflow API
# @dag(default_args=DEFAULT_ARGS,
#      schedule_interval='@daily',
#      # max_active_runs=1,
#      tags=['i-oralin-7'],
#      # Do run for all previous days
#      catchup=True)
# def i_oralin_7_hw5_ram_locations_taskflow():
