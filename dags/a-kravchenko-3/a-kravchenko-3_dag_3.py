"""Забирает из https://rickandmortyapi.com/api/location
данные о локации, считает количество резидентов и
помещает в GreenPlum a_kravchenko_3_ram_locations"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta

from airflow.providers.postgres.operators.postgres import PostgresOperator
from a_kravchenko_3_plugins.operators.top3_locations import Top3LocationsOperator


DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-kravchenko-3',
    'retries': 4,
    'retry_delay': timedelta(seconds=30),
    'max_retry_delay': timedelta(seconds=90),
}

with DAG("a-kravchenko-3_dag_3",
         schedule_interval=None,
         default_args=DEFAULT_ARGS,
         catchup=False,
         tags=['a-kravchenko-3']
         ) as dag:

    ram_top3_location = Top3LocationsOperator(
        task_id="ram_top3_locations"
    )

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="conn_greenplum_write",
        sql="""
            CREATE TABLE IF NOT EXISTS a_kravchenko_3_ram_location
            (
                id integer PRIMARY KEY,
                name varchar(1024),
                type varchar(1024),
                dimension varchar(1024),
                resident_cnt integer
            )
            DISTRIBUTED BY (id);
        """,
        autocommit=True,
    )

    trunc_table = PostgresOperator(
        task_id='trunc_table',
        postgres_conn_id="conn_greenplum_write",
        sql="""
            TRUNCATE TABLE a_kravchenko_3_ram_location         
        """,
        autocommit=True,
    )

    load = PostgresOperator(
        task_id='load_top3_locations',
        postgres_conn_id="conn_greenplum_write",
        sql="""
            INSERT INTO a_kravchenko_3_ram_location 
            VALUES {{ ti.xcom_pull(task_ids='ram_top3_locations') }}
        """,
        autocommit=True,
    )

    ram_top3_location >> create_table >> trunc_table >> load
