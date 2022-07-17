from airflow import DAG
from airflow.utils.dates import days_ago

import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from a_kazimirov_7_plugins.location_operator import LocationOperator


def create_table():
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    with conn.cursor() as cur:
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS public.a_kazimirov_7_ram_location (
                         id int, name varchar, type varchar, dimension varchar, resident_cnt int);
                    TRUNCATE TABLE public.a_kazimirov_7_ram_location;
                    COMMIT;"""
                    )

def insert_data(ti):
        result_location_info = ti.xcom_pull(key='return_value', task_ids='get_top_3_locations')
        logging.info(result_location_info)
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        with conn.cursor() as cur:
            for loc in result_location_info:
                query = f"""
                    INSERT INTO public.a_kazimirov_7_ram_location (id, name, type, dimension, resident_cnt)
                    VALUES ( {loc[0]}, '{loc[1]}', '{loc[2]}', '{loc[3]}', {loc[4]}
                           );"""
                cur.execute(query)
        conn.commit()
        conn.close()

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'poke_interval': 600
}

with DAG("a-kazimirov-7_task5_2",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         tags=['kazimirov'],
         max_active_runs=1
         ) as dag:

    start = DummyOperator(task_id='start')

    create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_table
    )

    get_top_3_locations = LocationOperator(
         task_id='get_top_3_locations'
    )

    insert_data = PythonOperator(
         task_id='insert_data',
         python_callable=insert_data
    )

    start >> create_table >> get_top_3_locations >> insert_data
