from airflow import DAG

from datetime import timedelta
import logging

from airflow.utils.dates import days_ago
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from random import randint
import pendulum

import requests
import logging
import pandas as pd

from airflow.models import BaseOperator
from a_svechkov_8_plugins.a_svechkov_8_rickandmorty_location_operator import ASvechkov8RickAndMortyLocationOperator

DEFAULT_ARGS = {
    'start_date': days_ago(0),
    'owner': 'a-svechkov-8',
    'poke_interval': 600
}


with DAG("a-svechkov-8_lesson_5_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-svechkov-8']
         ) as dag:

    #start = DummyOperator(task_id='start')

    create_ram_locations_table = PostgresOperator(
        task_id='create_ram_locations_table',
        sql=''' CREATE TABLE IF NOT EXISTS public.a_svechkov_8_ram_location (
                id INTEGER NOT NULL,
                name VARCHAR NOT NULL,
                type VARCHAR NOT NULL,
                dimension VARCHAR NOT NULL,
                resident_cnt INTEGER NOT NULL,
                PRIMARY KEY (id)
                ); ''',
        postgres_conn_id='conn_greenplum_write',
        dag=dag
    )

    delete_data = PostgresOperator(
        task_id='delete_data',
        sql="DELETE FROM public.a_svechkov_8_ram_location;",
        postgres_conn_id='conn_greenplum_write',
        autocommit=True,
        dag=dag
    )

    top3_ram_location_insert_table = ASvechkov8RickAndMortyLocationOperator(
        task_id='top3_ram_location_insert_table',
        table_name='public.a_svechkov_8_ram_location',
        conn_id='conn_greenplum_write'
    )


    create_ram_locations_table >> delete_data >> top3_ram_location_insert_table;




