from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from s_chertkov_plugins.schertkov_ram_top_locations_operator import SChertkovRamTopLocationsOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-chertkov',
    'poke_interval': 600
}


with DAG("s-chertkov-load-ram",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s-chertkov']
) as dag:

    get_top_locations = SChertkovRamTopLocationsOperator(
        task_id='schertkov-print-top-locations',
        limit=3
    )

    store_in_greenplum = PostgresOperator(
        task_id='create_or_clean_table',
        postgres_conn_id='conn_greenplum_write',
        sql=[
            '''
            CREATE TABLE IF NOT EXISTS "s_chertkov_ram_location"
            (
                id           INTEGER PRIMARY KEY,
                name         VARCHAR,
                type         VARCHAR,
                dimension    VARCHAR,
                resident_cnt INTEGER
            )
                DISTRIBUTED BY (id);''',
            '''TRUNCATE TABLE "s_chertkov_ram_location";''',
            '''INSERT INTO "s_chertkov_ram_location" VALUES {{ ti.xcom_pull(task_ids='schertkov-print-top-locations', key='return_value') }};'''

        ],
        autocommit=True
    )

    get_top_locations >> store_in_greenplum
