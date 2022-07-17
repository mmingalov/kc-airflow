"""
Загружаем данные по локациям из API Рика и Морти и выбираем топ-3 по количеству резидентов
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from a_troshin_8_plugins.a_troshin_8_ram_location_operator import ATroshin8RamLocationOperator

TABLE_NAME = 'public.a_troshin_8_ram_location'
GP_CONN = 'conn_greenplum_write'

DEFAULT_ARGS = {
    'start_date': days_ago(0),
    'owner': 'a.troshin-8',
    'poke_interval': 600
}


with DAG("a.troshin-8_ram_location_DAG",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a.troshin-8']
         ) as dag:

    start = DummyOperator(task_id='start')

    ram_locations_table_create = PostgresOperator(
        task_id='ram_locations_table_create',
        sql=f'''
                        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                        id INTEGER NOT NULL,
                        name VARCHAR NOT NULL,
                        type VARCHAR NOT NULL,
                        dimension VARCHAR NOT NULL,
                        resident_cnt INTEGER NOT NULL,
                        PRIMARY KEY (id)
                        );
                    ''',
        postgres_conn_id=GP_CONN,
        dag=dag
    )

    delete_prev_data_from_GP = PostgresOperator(
        task_id='delete_prev_data_from_GP',
        postgres_conn_id=GP_CONN,
        sql=f"DELETE FROM {TABLE_NAME};",
        autocommit=True,
        dag=dag
    )

    top3_location_to_GP = ATroshin8RamLocationOperator(
        task_id='top3_location_to_GP',
        gp_table_name=TABLE_NAME,
        gp_conn_id=GP_CONN
    )

    end = DummyOperator(task_id='end')

    start >> ram_locations_table_create >> delete_prev_data_from_GP >> top3_location_to_GP >> end