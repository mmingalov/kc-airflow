from airflow import DAG
from airflow.utils.dates import days_ago

import logging


from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from a_klimenko_4_plugins.a_klimenko_4_ram_max_residents import AllaRamMaxResidentsOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'poke_interval': 600,
    'owner': 'a-klimenko-4'
}

with DAG("a-klimenko-4_morty", schedule_interval='@daily',
         default_args=DEFAULT_ARGS, tags=['a-klimenko-4'], max_active_runs=1) as dag:

    start = DummyOperator(task_id='start')

    get_top_3_location = AllaRamMaxResidentsOperator(
         task_id='get_top_3_location'
    )


    def load_json_to_greenplum_func(ti):
        result_location_info = ti.xcom_pull(key='return_value', task_ids='get_top_3_location')
        insert_values = [f"({loc['id']}, '{loc['name']}', '{loc['type']}', '{loc['dimension']}', {loc['resident_cnt']})"
                         for loc in result_location_info]
        logging.info(insert_values)
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        truncate_sql = """TRUNCATE TABLE public.a_klimenko_4_ram_location"""
        pg_hook.run(truncate_sql, False)
        insert_sql = f"INSERT INTO public.a_klimenko_4_ram_location VALUES {','.join(insert_values)}"
        pg_hook.run(insert_sql, False)

    load_json_to_greenplum = PythonOperator(
         task_id='load_json_to_greenplum',
         python_callable=load_json_to_greenplum_func
    )

    start >> get_top_3_location >> load_json_to_greenplum
