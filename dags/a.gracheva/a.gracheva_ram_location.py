"""
Загружаем данные из API Рика и Морти
находим три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import requests

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from a_gracheva_plugins.a_gracheva_ram_location_operator import TopLocationsGrachevaOperator

def write_to_table(ti):
        result_location_info = ti.xcom_pull(key='return_value', task_ids='get_top_locations')
        logging.info(result_location_info)
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        with conn.cursor() as cur:
            for loc in result_location_info:
                query = f"""
                    INSERT INTO public."a.gracheva_ram_location" (id, name, type, dimension, resident_cnt)
                    VALUES ( {loc[0]}, '{loc[1]}', '{loc[2]}', '{loc[3]}', {loc[4]}
                           );"""
                cur.execute(query)
        conn.commit()
        conn.close()


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'poke_interval': 600
}


with DAG("gracheva_load_ram",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a.gracheva']
         ) as dag:

    start = DummyOperator(task_id='start')


    get_top_locations = TopLocationsGrachevaOperator(
        task_id='get_top_locations'
    )

    write_to_table = PythonOperator(
        task_id='write_to_table',
        python_callable = write_to_table
    )

    start >> get_top_locations >> write_to_table

