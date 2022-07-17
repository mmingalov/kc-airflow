from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from e_krupkina_plugins.e_krupkina_top3_rick_and_morty_locations import KrupkinaTopLocations


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'e-krupkina-3',
    'poke_interval': 600
}

with DAG("e_krupkina-3.rick_and_morty",
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['e-krupkina-3']
          ) as dag:

    start = DummyOperator(task_id= 'start')

    create_or_truncate_table = PostgresOperator(
        task_id='create_or_truncate_table',
        postgres_conn_id='conn_greenplum_write',
        sql=[
            '''
            CREATE TABLE IF NOT EXISTS "e-krupkina-3_ram_location"
            (
                id           INTEGER PRIMARY KEY,
                name         VARCHAR(256),
                type         VARCHAR(256),
                dimension    VARCHAR(256),
                resident_cnt INTEGER
            )
                DISTRIBUTED BY (id);''',
            '''TRUNCATE TABLE "e-krupkina-3_ram_location";'''
        ],
        autocommit=True
    )

    get_top_locations = KrupkinaTopLocations(
        task_id = 'top_locations_Rick_and_Morty'
    )

    finish = DummyOperator(task_id='finish')


start >> create_or_truncate_table >> get_top_locations >> finish