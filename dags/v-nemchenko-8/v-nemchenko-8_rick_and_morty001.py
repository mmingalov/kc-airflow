

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from v_nemchenko_8_plugins.v_nemchenko_operator import NemchenkoOperator


TABLE_NAME = 'v_nemchenko_8_ram_location'
CONN_ID = 'conn_greenplum_write'

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v-nemchenko-8',
    'poke_interval': 600
}

dag = DAG("v-nemchenko-8_rick_and_morty",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['v-nemchenko-8']
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

top3_locations = NemchenkoOperator(
    task_id='top3_locations',
    conn_id=CONN_ID,
    table_name=TABLE_NAME,
    dag=dag
)

create_table >> truncate_table >> top3_locations
