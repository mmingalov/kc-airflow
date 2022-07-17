"""
This DAG use the plugin for calculate top-3 of location with max count of heroes
and write this values to the table e_zhbanova_ram_location
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from airflow.operators.dummy_operator import DummyOperator


from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

from e_zhbanova_plugins.e_zhbanova_ram_location import ZhbanovaRamLocationOperator


DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'e-zhbanova',
    'poke_interval': 600,
    'depends_on_past': False
}


def load_top3_locations_func(**kwargs):
    # loading into table the result of top-3 location from XCom
    result_location_info = kwargs['ti'].xcom_pull(key='return_value', task_ids='get_top_3_location')

    # checking right loading
    logging.info(result_location_info)
    r_id = result_location_info['id']
    r_name = result_location_info['name']
    r_type = result_location_info['type']
    r_dimension = result_location_info['dimension']
    r_resident_cnt = result_location_info['resident_cnt']

    # opening the table and insert data into it
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    with conn.cursor() as cur:
        for i in range(3):
            query = f"INSERT INTO public.e_zhbanova_ram_location VALUES " \
                    f"({r_id[i]},'{r_name[i]}','{r_type[i]}','{r_dimension[i]}',{r_resident_cnt[i]})"
            cur.execute(query)
    conn.commit()
    conn.close()

with DAG("e-zhbanova-03-05",
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['e-zhbanova']
        ) as dag:

    start = DummyOperator(task_id='start')

    create_and_clean_table = PostgresOperator(
        # creating the table and cleaning it
        task_id="create_and_clean_table",
        postgres_conn_id='conn_greenplum_write',
        sql= ["""CREATE TABLE IF NOT EXISTS public.e_zhbanova_ram_location (
                        id INT,
                        name TEXT PRIMARY KEY,
                        type VARCHAR,
                        dimension VARCHAR, 
                        resident_cnt INT); """,
              "TRUNCATE TABLE public.e_zhbanova_ram_location"],
        autocommit=True
    )

    get_top_3_location = ZhbanovaRamLocationOperator(
        task_id='get_top_3_location',
        dag=dag
    )

    load_top3_locations = PythonOperator(
        task_id='load_top3_locations',
        python_callable=load_top3_locations_func,
        dag=dag
    )

    end = DummyOperator(task_id='end')

start >> create_and_clean_table >> get_top_3_location >> load_top3_locations >> end
