from airflow import DAG
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

from n_kotenko_3_plugins.kotenko_ram_location_operator import KotenkoRamLocationOperator




DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'kotenko',
    'poke_interval': 600
}

with DAG("kotenko_2_greenplum",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['kotenko']
         ) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    connection = 'conn_greenplum_write'
    table_name = 'n_kotenko_3_ram_location'

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id=connection,
        sql=f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY,
                name VARCHAR NOT NULL,
                type VARCHAR NOT NULL,
                dimension VARCHAR,
                resident_cnt INTEGER NOT NULL);
              """,
    )

    load_location_file = KotenkoRamLocationOperator(
        task_id='load_location_file'
    )


    def load_csv_to_gp_func():
        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.copy_expert("COPY dina_cbr FROM STDIN DELIMITER ';'", 'kotenko_ram.csv')


    load_location_csv_to_gp = PythonOperator(
        task_id='load_location_csv_to_gp',
        python_callable=load_csv_to_gp_func,
        dag=dag
    )

    start >> create_table >> load_location_file >> load_location_csv_to_gp >> end
