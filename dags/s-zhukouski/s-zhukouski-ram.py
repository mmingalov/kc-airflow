import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

from s_zhukouski_plugins.s_zhukouski_ram_operator import SZhukouskiRickAndMortyTopLocationOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 's-zhukouski',
    'poke_interval': 600
}


def save_result(**kwargs):
    ti = kwargs['ti']
    top = ti.xcom_pull(task_ids='get_top', key='result')

    logging.info('TOP')
    logging.info(top)

    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

    sql_create_table = '''
    CREATE TABLE IF NOT EXISTS public."s-zhukouski_ram_location" (
        id int4 NOT NULL,
        "name" varchar(256) NULL,
        "type" varchar(256) NULL,
        dimension varchar(256) NULL,
        resident_cnt int4 NULL,
        CONSTRAINT "s-zhukouski_ram_location_pk" PRIMARY KEY (id)
    )
    DISTRIBUTED BY (id);
    
    truncate table public."s-zhukouski_ram_location" 
    '''
    pg_hook.run(sql_create_table, False)

    top_list = [(item['id'], item['name'], item['type'], item['dimension'], item['resident_cnt'])
                for item in top]

    pg_hook.insert_rows(
        table='public."s-zhukouski_ram_location"',
        rows=top_list,
        target_fields=['id', 'name', 'type', 'dimension', 'resident_cnt']
    )


with DAG("s-zhukouski-ram",
         schedule_interval='@once',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['s-zhukouski']
         ) as dag:

    top_operator = SZhukouskiRickAndMortyTopLocationOperator(
        task_id='get_top',
        top=3
    )

    save_result_operator = PythonOperator(
        task_id='save_result',
        python_callable=save_result
    )

    top_operator >> save_result_operator
