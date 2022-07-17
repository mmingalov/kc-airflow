"""
Searching for top-3 locations in Rick And Morty TV series
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from a_kolesnik_3_plugins.a_kolesnik_3_toplocations import KolesnikTopLocationsOperator


TABLE_NAME = 'a_kolesnik_3_ram_location'
CONN_ID = 'conn_greenplum_write'

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-kolesnik-3',
    'poke_interval': 600
}

dag = DAG("a-kolesnik-3_rickandmorty",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-kolesnik-3']
          )

create_table = PostgresOperator(
    task_id='create_table',
    sql=f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL4 PRIMARY KEY,
            name VARCHAR NOT NULL,
            type VARCHAR NOT NULL,
            dimension VARCHAR NOT NULL,
            resident_cnt INT4 NOT NULL);
    """,
    autocommit=True,
    postgres_conn_id=CONN_ID,
    dag=dag
)

truncate_table = PostgresOperator(
    task_id='truncate_table',
    postgres_conn_id=CONN_ID,
    sql=f"TRUNCATE TABLE {TABLE_NAME};",
    autocommit=True,
    dag=dag
)

export_top_locations = KolesnikTopLocationsOperator(
    task_id='export_top_locations',
    top_n=3,
    conn_id=CONN_ID,
    table_name=TABLE_NAME,
    dag=dag
)

create_table >> truncate_table >> export_top_locations
