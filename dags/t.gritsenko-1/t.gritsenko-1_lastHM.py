################################################

"RAM"

###############################################

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from gritsenko_plugins.gritsenko_top3_rick_and_morty_location import GritsenkoLocationOperator

DEFAULT_ARGS = {
    'start_date': days_ago(0),
    'owner': 'Karpov',
}

TABLE_NAME = 't.gritsenko-1_ram_location'
GREENPLUM_CONN = 'conn_greenplum_write'

with DAG('t.gritsenko-1_LASHM',
         schedule_interval='0 0 * * *',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['karpov']
         ) as dag:

    start = DummyOperator(task_id='start')
    finish = DummyOperator(task_id='finish')

########## IF NOT EXISTS - Истина при отсутсвии

    drop_df = PostgresOperator(
        task_id='drop_df',
        postgres_conn_id=GREENPLUM_CONN,
        sql=f"DROP TABLE {TABLE_NAME} IF EXISTS;",
        autocommit=True
    )

    create_df = PostgresOperator(
        task_id='create_df',
        sql=f"""
            CREATE TABLE {TABLE_NAME} (
                id  VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                type VARCHAR NOT NULL,
                dimension VARCHAR NOT NULL,
                resident_cnt VARCHAR NOT NULL);
        """,
        postgres_conn_id=GREENPLUM_CONN
    )

    load_ram_locations = GritsenkoLocationOperator(
        task_id='load_ram_locations',
        gp_table_name=TABLE_NAME,
        gp_conn_id=GREENPLUM_CONN
    )

    start >> drop_df >> create_df >> load_ram_locations >> finish