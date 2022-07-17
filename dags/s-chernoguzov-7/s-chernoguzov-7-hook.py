"""
Get data by week day
"""

from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from s_chernoguzov_7_plugins.top_location_sc import TopLocation


DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 's-chernoguzov-7',
    'poke_interval': 600
}

with DAG("s-chernoguzov-7-hook",
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['s-chernoguzov-7']
          ) as dag:

    dummy = DummyOperator(task_id='dummy')

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="conn_greenplum_write",
        sql=["""CREATE TABLE IF NOT EXISTS public.s_chernoguzov_7_ram_location (
                                id           SERIAL PRIMARY KEY,
                                name         VARCHAR,
                                type         VARCHAR,
                                dimension    VARCHAR,
                                resident_cnt INTEGER
                )
                DISTRIBUTED BY (id);
            """,
            'TRUNCATE TABLE public.s_chernoguzov_7_ram_location;'
        ],
        autocommit=True
    )

    top_location_insert = TopLocation(
        task_id="top_location_insert"
    )

    dummy >> create_table >> top_location_insert