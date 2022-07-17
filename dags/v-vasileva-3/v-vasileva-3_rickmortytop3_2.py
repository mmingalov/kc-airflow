from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago

import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from v_vasileva_3_plugins.v_vasileva_3_location_operator import TopLocationOperator



default_args = {
    'start_date': days_ago(1),
    'owner': 'v-vasileva-3',
    'retries': 5,
    'retry_delay': timedelta(seconds=30),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=1),
}


dag_params = {
    'dag_id': 'v-vasileva-3_rickmortytop3_2',
    'catchup': False,
    'default_args': default_args,
    'tags': ['v-vasileva-3'],
    'schedule_interval': None,
}


with DAG(**dag_params) as dag:


    get_top_3_location = TopLocationOperator(
         task_id='get_top_3_location'
    )

    create_table_if_not_exists = PostgresOperator(
        task_id='create_table_if_not_exists',
        postgres_conn_id='conn_greenplum_write',
        sql="""
            CREATE TABLE IF NOT EXISTS public.v_vasileva_3_rickmortytop3
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

    load_top3_locations_gp = PostgresOperator(
        task_id='load_top3_locations_gp',
        postgres_conn_id='conn_greenplum_write',
        sql=[
            "TRUNCATE TABLE v_vasileva_3_rickmortytop3",
            "INSERT INTO v_vasileva_3_rickmortytop3 VALUES {{ ti.xcom_pull(task_ids='get_top_3_location') }}",
        ],
        autocommit=True,
    )



    get_top_3_location >> create_table_if_not_exists >> load_top3_locations_gp