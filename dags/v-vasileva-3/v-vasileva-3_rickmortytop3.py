from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago

import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from v_vasileva_3_plugins.v_vasileva_3_location_operator import TopLocationOperator

def load_json_to_greenplum_func(ti):

        result_location_info = ti.xcom_pull(key='return_value', task_ids='get_top_3_location')

        logging.info(result_location_info)
        logging.info(result_location_info['id'][0])
        logging.info(result_location_info['name'][0])
        logging.info(result_location_info['type'][0])

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.v_vasileva_3_location (
                     id int, name text, type text, dimension text, resident_cnt int);
                TRUNCATE TABLE public.v_vasileva_3_location;"""
                )
            for i in range(3):
                query = f"""
                INSERT INTO public.v_vasileva_3_location (id, name, type, dimension, resident_cnt)
                VALUES (    {result_location_info['id'][i]}
                          , '{result_location_info['name'][i]}'
                          , '{result_location_info['type'][i]}'
                          , '{result_location_info['dimension'][i]}'
                          , {result_location_info['resident_cnt'][i]}
                        );"""
                cur.execute(query)
        conn.commit()
        conn.close()


default_args = {
    'start_date': days_ago(1),
    'owner': 'v-vasileva-3',
    'retries': 5,
    'retry_delay': timedelta(seconds=30),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=1),
}


dag_params = {
    'dag_id': 'v-vasileva-3_rickmortytop3',
    'catchup': False,
    'default_args': default_args,
    'tags': ['v-vasileva-3'],
    'schedule_interval': None,
}


with DAG(**dag_params) as dag:

    begin = DummyOperator(task_id='begin')

    get_top_3_location = TopLocationOperator(
         task_id='get_top_3_location'
    )

    load_json_to_greenplum = PythonOperator(
         task_id='load_json_to_greenplum',
         python_callable=load_json_to_greenplum_func
    )

    begin >> get_top_3_location >> load_json_to_greenplum