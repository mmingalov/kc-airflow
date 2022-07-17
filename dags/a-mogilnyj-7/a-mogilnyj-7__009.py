####
# Создайте в GreenPlum'е таблицу с названием "<ваш_логин>_ram_location" с полями id, name, type, dimension, resident_cnt
####
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from amogilnyj_7_plugins.mogilnyj_7_009operator import Top3LocationOperator

def load_json_to_greenplum_func(ti):

        result_location_info = ti.xcom_pull(key='return_value', task_ids='get_top_3_location')

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.a_mogilnyj_7_ram_location (
                     id int, name varchar, type varchar, dimension varchar, resident_cnt int);
                TRUNCATE TABLE public.a_mogilnyj_7_ram_location;
                COMMIT;"""
                )
            for i in range(3):
                query = f"""
                    INSERT INTO public.a_mogilnyj_7_ram_location  (id, name, type, dimension, resident_cnt)
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

with DAG("a-mogilnyj-7__012",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         tags=['a-mogilnyj-7__012'],
         max_active_runs=1
         ) as dag:

    start = DummyOperator(task_id='start')
    stop = DummyOperator(task_id='stop')

    get_top_3_location = Top3LocationOperator(
         task_id='get_top_3_location'
    )

    load_json_to_greenplum = PythonOperator(
         task_id='load_json_to_greenplum',
         python_callable=load_json_to_greenplum_func
    )

    start >> get_top_3_location >> load_json_to_greenplum >> stop
