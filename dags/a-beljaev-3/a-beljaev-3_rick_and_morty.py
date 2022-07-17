from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

from a_beljaev_3_plugins.rick_and_morty_operator import TopLocationsRickAndMorty

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-beljaev-3',
    'poke_interval': 600
}

with DAG("a-beljaev-3_rick_and_morty",
         schedule_interval=None,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-beljaev-3', 'rick_and_morty']
         ) as dag:

    start_process = DummyOperator(task_id='start_process')

    create_or_truncate_table = PostgresOperator(
        task_id='create_or_truncate_table',
        postgres_conn_id='conn_greenplum_write',
        sql=[
            '''
            CREATE TABLE IF NOT EXISTS "a-beljaev-3_ram_location"
            (
                id           INTEGER PRIMARY KEY,
                name         VARCHAR(256),
                type         VARCHAR(256),
                dimension    VARCHAR(256),
                resident_cnt INTEGER
            )
                DISTRIBUTED BY (id);''',
            '''TRUNCATE TABLE "a-beljaev-3_ram_location";'''
        ],
        autocommit=True
    )

    top_locations_rick_and_morty = TopLocationsRickAndMorty(task_id='top_locations_rick_and_morty')

    end_process = DummyOperator(task_id='end_process')

    start_process >> create_or_truncate_table >> top_locations_rick_and_morty >> end_process
