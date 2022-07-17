"""
Загружаем данные из API Рика и Морти
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import requests

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException
from m_averbah_4_plugins.m_averbah_4_find_locations_operator import Maverbah4FindLocationsOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'mahw',
    'poke_interval': 600
}

with DAG("m_averbah_4_ram_location",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['mahw']
         ) as dag:

    start = DummyOperator(task_id='start')

    get_top_3_location = Maverbah4FindLocationsOperator(
        task_id='get_3_locations'
    )

    def load_to_greenplum_func(**kwargs):
        locations = kwargs['templates_dict']['implicit'].replace('[[','').replace(']]','').split('], [')
        pg_hook=PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute('TRUNCATE TABLE m_averbah_4_ram_location')
        for loc in locations:
            loc=loc.split(', ')
            cursor.execute(f'INSERT INTO m_averbah_4_ram_location VALUES \
            ({loc[0]},{loc[1]},{loc[2]},{loc[3]},{(loc[4])})')
        conn.commit()
        cursor.close()
        conn.close()
        conn.close()

    load_to_greenplum=PythonOperator(
        task_id='load_to_greenplum',
        python_callable=load_to_greenplum_func,
        templates_dict={'implicit': '{{ ti.xcom_pull(task_ids="get_3_locations") }}'}
    )

    start >> get_top_3_location >> load_to_greenplum

