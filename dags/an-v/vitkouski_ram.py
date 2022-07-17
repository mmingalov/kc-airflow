"""
Load currency rates and save to GP
"""
import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from an_v_plugins.ram_plugins import VitkouskiRamLocationCountOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'an-v',
    'poke_interval': 600
}

with DAG("vitkouski_ram",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['an-v']
         ) as dag:
    def upload_to_gp(**kwargs):
        ti = kwargs['ti']
        locations = ti.xcom_pull(key='top_locations', task_ids='extract_locations')
        logging.info(f'locations: {locations}')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run('CREATE TABLE IF NOT EXISTS an_v_ram_location '
                    '(id int, name text, type text, dimension text, resident_cnt int);')
        pg_hook.run('truncate an_v_ram_location;')
        top_list = [(item['id'], item['name'], item['type'], item['dimension'], item['resident_cnt'])
                    for item in locations]
        pg_hook.insert_rows(
            table='an_v_ram_location',
            rows=top_list,
            target_fields=['id', 'name', 'type', 'dimension', 'resident_cnt']
        )


    start = DummyOperator(task_id='start')
    extract_locations = VitkouskiRamLocationCountOperator(task_id='extract_locations', do_xcom_push=True)
    load_to_gp = PythonOperator(
        task_id='load_to_gp',
        python_callable=upload_to_gp
    )

    start >> extract_locations >> load_to_gp
