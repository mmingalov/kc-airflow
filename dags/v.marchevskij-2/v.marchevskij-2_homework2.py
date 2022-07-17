from datetime import timedelta, datetime

import airflow
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from v_marchevskij_plugins.ram_locations_operator import RamLocationsOperator

DEFAULT_ARGS = {
    'owner': 'v.marchevskij-2',
    'depends_on_past': False,
    'wait_for_downstream': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=15),
    'start_date': airflow.utils.dates.days_ago(0),
    'end_date': datetime(2021, 12, 13),
    'trigger_rule': 'all_success'
}

GP_TABLE_NAME = 'marchevskij2_ram_location'
GP_CONN_ID = 'conn_greenplum_write'

with DAG(
    dag_id='marchevskiy_homework3',
    description='A homework 3 DAG.',
    schedule_interval='0 0 * * 1-5',
    default_args=DEFAULT_ARGS,
    max_active_runs=1
) as dag:

    start = DummyOperator(task_id='start')

    create_table = PostgresOperator(
        task_id='create_table',
        sql=f"""
            CREATE TABLE IF NOT EXISTS {GP_TABLE_NAME} (
                id  SERIAL4 PRIMARY KEY,
                name VARCHAR NOT NULL,
                type VARCHAR NOT NULL,
                dimension VARCHAR NOT NULL,
                resident_cnt INT4 NOT NULL);
        """,
        postgres_conn_id=GP_CONN_ID
    )

    get_top_3_locations = RamLocationsOperator(
        task_id='get_top_3_locations',
        top_n=3
    )

    def insert_top_3_locations_func(**kwargs):
        top_3_locations_list = kwargs['ti'].xcom_pull(task_ids='get_top_3_locations', key='return_value')
        pg_hook = PostgresHook(postgres_conn_id=GP_CONN_ID)
        for location_dict in top_3_locations_list:
            _id = location_dict["id"]
            _name = location_dict["name"]
            _type = location_dict["type"]
            _dimension = location_dict["dimension"]
            _resident_cnt = len(location_dict["residents"])
            pg_hook.run(f"DELETE FROM {GP_TABLE_NAME} WHERE id={_id};")
            pg_hook.run(
                f"INSERT INTO {GP_TABLE_NAME} (id, name, type, dimension, resident_cnt) VALUES ('{_id}', '{_name}', '{_type}', '{_dimension}', {_resident_cnt});")

    insert_top_3_locations = PythonOperator(
        task_id='insert_top_3_locations',
        python_callable=insert_top_3_locations_func,
        provide_context=True
    )

    start >> create_table >> get_top_3_locations >> insert_top_3_locations
