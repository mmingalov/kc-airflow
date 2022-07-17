"""
Lesson5. Get top locations https://rickandmortyapi.com/documentation/#location
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from k_chudakova_13_plugins.k_chudakova_operator import ChudakovaOperator


TABLE_NAME = 'k_chudakova_13_ram_top_locations'
CONN_ID = 'conn_greenplum_write'

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'k.chudakova-13',
    'catchup': True,
    'poke_interval': 600
}

with DAG("k.chudakova-13_ram_top_locations",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['k.chudakova-13']
) as dag:

    create_table_if_not_exists = PostgresOperator(
        task_id='create_table_if_not_exists',
        sql=f"""
                CREATE TABLE IF NOT EXISTS public.{TABLE_NAME} (
                id              int4 PRIMARY KEY,
                name            varchar(100) NOT NULL,
                type            varchar(100) NOT NULL,
                dimension       varchar(100) NOT NULL,
                resident_cnt    int4 NOT NULL,
                UNIQUE(id, name, type, dimension, resident_cnt)
                )
                DISTRIBUTED BY (id);
            """,
        autocommit=True,
        postgres_conn_id=CONN_ID,
    )

    truncate_table = PostgresOperator(
        task_id='truncate_table',
        postgres_conn_id=CONN_ID,
        sql=f"TRUNCATE TABLE public.{TABLE_NAME};",
        autocommit=True,
    )

    write_top_locations = ChudakovaOperator(
        task_id='write_top_locations',
        conn_id=CONN_ID,
        table_name=TABLE_NAME,
    )

create_table_if_not_exists >> truncate_table >> write_top_locations
