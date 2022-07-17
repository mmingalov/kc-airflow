"""
Загружаем данные из API Рика и Морти
"""

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from malahova_plugins.malahova_count_operator import InnaMalahovaCountOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'i-malahova-3',
    'poke_interval': 600
}

TABLE_NAME = 'i_malahova_3_ram_location'
CONN = 'conn_greenplum_write'

with DAG("i-malahova-3_ram_api",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['malahova']
         ) as dag:

    start = DummyOperator(task_id='start')

    create_table = PostgresOperator(
        task_id='create_table',
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id  SERIAL4 PRIMARY KEY,
                name VARCHAR NOT NULL,
                type VARCHAR NOT NULL,
                dimension VARCHAR NOT NULL,
                resident_cnt INT4 NOT NULL);
        """,
        postgres_conn_id=CONN
    )

    delete_rows = PostgresOperator(
        task_id='delete_rows',
        postgres_conn_id=CONN,
        sql=f"TRUNCATE {TABLE_NAME};"
    )

    count_residents = InnaMalahovaCountOperator(
        task_id='count_residents',
        table_name=TABLE_NAME,
        conn=CONN
    )


    start >> create_table >> delete_rows >> count_residents
