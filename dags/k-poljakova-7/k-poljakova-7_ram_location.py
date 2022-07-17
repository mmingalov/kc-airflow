

from airflow import DAG
from airflow.utils.dates import days_ago

import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from k_poljakova_7_plugins.poljakova_ram_location_operator import PoljakovaRamLocationOperator

def load_json_to_greenplum_func(ti):

        result_location_info = ti.xcom_pull(key='return_value', task_ids='get_top_3_location')

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.k_poljakova_7_ram_location (
                     id int, name varchar, type varchar, dimension varchar, resident_cnt int);
                TRUNCATE TABLE public.k_poljakova_7_ram_location;
                COMMIT;"""
                )
            for i in range(3):
                query = f"""
                    INSERT INTO public.k_poljakova_7_ram_location (id, name, type, dimension, resident_cnt)
                    VALUES (    {result_location_info['id'][i]}
                              , '{result_location_info['name'][i]}'
                              , '{result_location_info['type'][i]}'
                              , '{result_location_info['dimension'][i]}'
                              , {result_location_info['resident_cnt'][i]}
                            );"""
                cur.execute(query)
        conn.commit()
        conn.close()

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'poke_interval': 600
}

with DAG("k-poljakova-7_ram_location",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         tags=['k-poljakova-7'],
         max_active_runs=1
         ) as dag:

    start = DummyOperator(task_id='start')

    get_top_3_location = PoljakovaRamLocationOperator(
         task_id='get_top_3_location'
    )

    load_json_to_greenplum = PythonOperator(
         task_id='load_json_to_greenplum',
         python_callable=load_json_to_greenplum_func
    )

    start >> get_top_3_location >> load_json_to_greenplum
