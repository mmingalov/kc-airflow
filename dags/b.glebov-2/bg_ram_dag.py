import datetime as dt

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from glebov_plugins.glebov_top3_rick_and_morty_location import (
    RamApiLocationOperator
)
from sqlalchemy import create_engine

TABLE_NAME = 'b_glebov_2_ram_location'
GREENPLUM_CONN = 'conn_greenplum_write'

DEFAULT_ARGS = {
    'start_date': days_ago(0),
    'owner': 'b.glebov-2',
    'poke_interval': 600
}

dag = DAG(
    "bg_ram_dag",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['karpov', 'glebov', 'ram', 'api'],
)

start = DummyOperator(
    task_id='start',
    dag = dag,
)
end = DummyOperator(
    task_id='end',
    dag = dag,
)

get_top3_location = RamApiLocationOperator(
    task_id='get_top3_location',
    url='https://rickandmortyapi.com/api/location',
    dag = dag,
    )

#drop_df = PostgresOperator(
#    task_id='drop_df',
#    postgres_conn_id=GREENPLUM_CONN,
#    sql=f"DROP TABLE {TABLE_NAME} IF EXISTS;",
#    autocommit=True,
#    dag = dag,
#    )

create_df = PostgresOperator(
    task_id='create_df',
    sql=f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id  VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL,
            type VARCHAR NOT NULL,
            dimension VARCHAR NOT NULL,
            resident_cnt VARCHAR NOT NULL);
        """,
    postgres_conn_id=GREENPLUM_CONN,
    dag = dag,
    )

start >> get_top3_location >> create_df >> end

dag.doc_md = __doc__
get_top3_location.doc_md = """Gets top-3 locations with residents"""
create_df.md = """Puts top-3 locations to db"""