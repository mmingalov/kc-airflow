from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from m_ilmetov_plugins.operators.rickandmorty_top3_locations_operator import RickAndMortyLocationOperator

default_args = {
    'start_date': days_ago(3) ,
    'owner': 'm-ilmetov-6',
    'wait_for_downstream': True,
}

dag_params = {
    'dag_id': 'm-ilmetov-6-2',
    'catchup': True,
    'default_args': default_args,
    'schedule_interval': '@daily',
    'tags': ['m-ilmetov-6'],
}

with DAG(**dag_params) as dag:

    start = DummyOperator (
        task_id = 'start',
    )

    rickandmorty_top3_location = RickAndMortyLocationOperator(
        task_id='rickandmorty_top3_location',
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql="""
            CREATE TABLE IF NOT EXISTS m_ilmetov_ram_location
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

    load = PostgresOperator(
        task_id='load',
        postgres_conn_id='conn_greenplum_write',
        sql=[
            "TRUNCATE TABLE m_ilmetov_ram_location",
            "INSERT INTO m_ilmetov_ram_location VALUES {{ ti.xcom_pull(task_ids='rickandmorty_top3_location') }}",
        ],
        autocommit=True,
    )


    end = DummyOperator(
        task_id='end',
    )

    start >> rickandmorty_top3_location >> create_table >> load >> end
