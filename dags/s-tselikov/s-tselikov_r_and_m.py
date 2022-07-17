from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

from s_tselikov_plugins.s_tselikov_r_and_m_operator import TopLocationsRAndM

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 's-tselikov',
    'poke_interval': 600
}

with DAG("s-tselikov_r_and_m",
         schedule_interval=None,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['s-tselikov']
         ) as dag:

    start_process = DummyOperator(task_id='start_process')

    create_or_truncate = PostgresOperator(
        task_id='create_or_truncate',
        postgres_conn_id='conn_greenplum_write',
        sql=[
            '''
            CREATE TABLE IF NOT EXISTS "s-tselikov_ram_location"
            (
                id           INTEGER PRIMARY KEY,
                name         VARCHAR(256),
                type         VARCHAR(256),
                dimension    VARCHAR(256),
                resident_cnt INTEGER
            )
                DISTRIBUTED BY (id);''',
            '''TRUNCATE TABLE "s-tselikov_ram_location";'''
        ],
        autocommit=True
    )

    top_locations_r_and_m = TopLocationsRAndM(task_id='top_locations_r_and_m')

    end_process = DummyOperator(task_id='end_process')

    start_process >> create_or_truncate >> top_locations_r_and_m >> end_process
