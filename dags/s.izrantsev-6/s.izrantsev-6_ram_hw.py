"""
Гутен даг
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
import logging
import requests

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from s_izrantsev_6_plugins.operators.s_izrantsev_6_ram_operator import IzrantsevRickAndMortyOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'Izrantsev',
    'poke_interval': 600
}

with DAG("s_izrantsev_6_ram_hw",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['izrantsev', 'Rick&Morty', 'ram']
         ) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    three_most_char_locations = IzrantsevRickAndMortyOperator(
        task_id='three_most_char_locations'
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql=['''
            create table if not exists s_izrantsev_6_ram_locations(
            id int
            ,name varchar
            ,type varchar 
            ,dimension varchar 
            ,resident_cnt int
            )
            distributed by (id);
            ''']
    )

    load_data = PostgresOperator(
        task_id='load_data_to_greenplum',
        postgres_conn_id='conn_greenplum_write',
        sql=['truncate s_izrantsev_6_ram_locations;',
             '''insert into s_izrantsev_6_ram_locations 
            values {{ ti.xcom_pull(task_ids='three_most_char_locations') }}
        ''']
    )


    def gp_query(**kwargs):
        hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f'SELECT * FROM s_izrantsev_6_ram_locations')
        query_res = cursor.fetchall()
        kwargs['ti'].xcom_push(value=query_res, key='result')
        logging.info('--------------------------------------------------------\n')
        logging.info('**** Result of query is: {} ****'.format(query_res))
        logging.info('--------------------------------------------------------\n')


    gp_query = PythonOperator(
        task_id='gp_query',
        python_callable=gp_query,
    )

start >> three_most_char_locations >> create_table >> load_data >> gp_query >> end
