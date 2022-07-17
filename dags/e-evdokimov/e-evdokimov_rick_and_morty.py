from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from e_evdokimov_plugins.rick_and_morty_operator import TopLocationsRnM

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'e-evdokimov',
    'poke_interval': 600
}

with DAG("e-evdokimov_rick_and_morty",
         schedule_interval=None,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['e-evdokimov']
         ) as dag:

    create_or_truncate_table = PostgresOperator(
        task_id='create_or_truncate_table',
        postgres_conn_id='conn_greenplum_write',
        sql=[
            '''
            CREATE TABLE IF NOT EXISTS "e-evdokimov_ram_location"
            (
                id           INTEGER PRIMARY KEY,
                name         VARCHAR(256),
                type         VARCHAR(256),
                dimension    VARCHAR(256),
                resident_cnt INTEGER
            )
                DISTRIBUTED BY (id);''',
            '''TRUNCATE TABLE "e-evdokimov_ram_location";'''
        ],
        autocommit=True
    )

    top_locations = TopLocationsRnM(task_id='top_locations')

    create_or_truncate_table >> top_locations