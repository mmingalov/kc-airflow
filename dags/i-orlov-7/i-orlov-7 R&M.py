

from airflow import DAG
from airflow.utils.dates import days_ago

import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from i_orlov_7_plugins.mn_ram_top_location_operator import MnRamTopLocationOperator

# load_json_to_greenplum
def load_json_to_greenplum_func(ti):
        # сохраненный в XCom результат с топ 3 локациями
        result_location_info = ti.xcom_pull(key='return_value', task_ids='get_top_3_location')
        # весь JSON + несколько значений в качестве примера
        # logging.info(result_location_info)
        #pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        #pg_hook.run("""
        #            CREATE TABLE IF NOT EXISTS public.a_jakovenko_7_ram_location (
        #                 id int, name varchar, type varchar, dimension varchar, resident_cnt int);
        #            TRUNCATE TABLE public.a_jakovenko_7_ram_location;""", True)
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.i_orlov_7_ram_location (
                     id int, name varchar, type varchar, dimension varchar, resident_cnt int);
                TRUNCATE TABLE public.i_orlov_7_ram_location;
                COMMIT;"""
                )
            for i in range(3): # топ 3 локации
                query = f"""
                    INSERT INTO public.i_orlov_7_ram_location (id, name, type, dimension, resident_cnt)
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

with DAG("i_orlov_7_ram",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         tags=['i_orlov_7_ram'],
         max_active_runs=1
         ) as dag:

    start = DummyOperator(task_id='start')

    get_top_3_location = MnRamTopLocationOperator(
         task_id='get_top_3_location'
    )

    load_json_to_greenplum = PythonOperator(
         task_id='load_json_to_greenplum',
         python_callable=load_json_to_greenplum_func
    )

    start >> get_top_3_location >> load_json_to_greenplum
