"""
Get top 3 locations
"""
import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from n_paveleva_7_plugins.n_paveleva_7_location_operator import N_Paveleva_Location_Operator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'n_paveleva',
    'poke_interval': 600
}

with DAG("n_paveleva_location",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['n_paveleva']
         ) as dag:

    def write_to_table(**kwargs):
        ti = kwargs['ti']
        locations = ti.xcom_pull(key='top3_locations', task_ids='get_locations')
        logging.info(f'locations: {locations}')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run('drop table if exists n_paveleva_location;')
        pg_hook.run('create table if not exists n_paveleva_location'
                    '(id int, name varchar(150), type varchar(150), dimension varchar(150), resident_cnt int);')
        location_list = [(item['id'], item['name'], item['type'], item['dimension'], item['resident_cnt'])
                    for item in locations]
        pg_hook.insert_rows(
            table='n_paveleva_location',
            rows=location_list,
            target_fields=['id', 'name', 'type', 'dimension', 'resident_cnt']
        )

    start = DummyOperator(task_id='start')

    get_locations = N_Paveleva_Location_Operator(task_id='get_locations', do_xcom_push=True)

    save_to_tbl = PythonOperator(
        task_id='save_to_table',
        python_callable=write_to_table
    )

    start >> get_locations >> save_to_tbl