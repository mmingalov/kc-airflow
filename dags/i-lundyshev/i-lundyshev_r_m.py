"""
Данный DAG использует плагин для вычисления трех локаций мультфильма Рик и Морти с максимальным
количеством персонажей и записывает их в таблицу

"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging


from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

from i_lundyshev_plugins.i_lundyshev_r_m_operator import LundyshevRamLocationOperator


DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'i-lundyshev',
    'poke_interval': 600,
    'depends_on_past': False
}


def load_top3_locations_gp_func(**kwargs):
    # сохраненный в XCom результат с топ 3 локациям загружаю в таблицу
    result_location_info = kwargs['ti'].xcom_pull(key='return_value', task_ids='get_top_3_location')

    logging.info(result_location_info) #проверяю, что всё открылось
    r_id = result_location_info['id']
    r_name = result_location_info['name']
    r_type = result_location_info['type']
    r_dimension = result_location_info['dimension']
    r_resident_cnt = result_location_info['resident_cnt']

    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    with conn.cursor() as cur:
        for i in range(3):
            query = f"INSERT INTO public.i_lundyshev_ram_location VALUES ({r_id[i]},'{r_name[i]}','{r_type[i]}','{r_dimension[i]}',{r_resident_cnt[i]})"
            cur.execute(query)
    conn.commit()
    conn.close()

with DAG("i-lundyshev_r_m",
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['i-lundyshev']
        ) as dag:

    create_and_clean_table = PostgresOperator(
        #создаю таблицу
        task_id="create_and_clean_table",
        postgres_conn_id='conn_greenplum_write',
        sql= ["""CREATE TABLE IF NOT EXISTS public.i_lundyshev_ram_location (
                        id INT,
                        name TEXT PRIMARY KEY,
                        type VARCHAR,
                        dimension VARCHAR, 
                        resident_cnt INT); """,
              "TRUNCATE TABLE public.i_lundyshev_ram_location"],
        autocommit=True
    )

    top_3_location = LundyshevRamLocationOperator(
        task_id='get_top_3_location',
        dag=dag
    )

    load_top3_locations_gp = PythonOperator(
        task_id='load_top3_locations_gp',
        python_callable=load_top3_locations_gp_func,
        dag=dag
    )

create_and_clean_table >> top_3_location >> load_top3_locations_gp

dag.doc_md = __doc__

load_top3_locations_gp.doc_md = """ Этот таск пишет в таблицу ответ"""
create_and_clean_table.doc_md = """Этот таск создает таблицу если надо и чистит её"""
top_3_location.doc_md = """Этот таск использует написанный оператор по расчету трёх самых населенных локаций"""