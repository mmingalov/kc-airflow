import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from j_tsyrlin_3_plugins.j_tsyrlin_3_location_operator import JTsyrlinLocationOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'poke_interval': 600
}

GP_TABLE_NAME = 'j_tsyrlin_3_top_locations'
GP_CONN_ID = 'conn_greenplum_write'
GP_TOP_N = 3

with DAG("j_tsyrlin_3_lesson7", schedule_interval='@daily',
         default_args=DEFAULT_ARGS, tags=['j-tsyrlin-3'], max_active_runs=1) as dag:
    start = DummyOperator(task_id='start')

    create_table = PostgresOperator(
        task_id='create_table',
        sql=f"""
                CREATE TABLE IF NOT EXISTS {GP_TABLE_NAME} (
                    id  integer PRIMARY KEY,
                    name text NOT NULL,
                    type text NOT NULL,
                    dimension text NOT NULL,
                    resident_cnt integer NOT NULL);
            """,
        postgres_conn_id=GP_CONN_ID
    )

    truncate_table = PostgresOperator(
        task_id='truncate_table',
        postgres_conn_id=GP_CONN_ID,
        sql=f"DELETE FROM {GP_TABLE_NAME};",
        autocommit=True
    )

    upload_table = JTsyrlinLocationOperator(
        task_id='upload_new_table',
        gp_table_name=GP_TABLE_NAME,
        gp_conn=GP_CONN_ID,
        top_n=GP_TOP_N)

    finish = DummyOperator(task_id='finish')

    start >> create_table >> truncate_table >> upload_table >> finish
