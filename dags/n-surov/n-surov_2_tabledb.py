"""
DAG для тестирования работы создания и загрузки данных в таблицу
"""

from airflow import DAG
import logging
import datetime

from airflow.providers.postgres.operators.postgres import PostgresOperator

DEFAULT_ARGS = {
    'owner': 'n-surov',
    'poke_interval': 600
}

TABLE_NAME = 'n_surov_testPostgresOperator'
CONN_ID = 'conn_greenplum_write'
begin_date = '2020-01-01'
end_date = '2020-12-31'


with DAG("n-surov_1_tabletest",
         start_date=datetime.datetime(2022, 2, 12),
         schedule_interval='@once',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['n-surov']
         ) as dag:

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id=CONN_ID,
        sql=F"""
               CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
               pet_id SERIAL PRIMARY KEY,
               name VARCHAR NOT NULL,
               pet_type VARCHAR NOT NULL,
               birth_date DATE NOT NULL,
               OWNER VARCHAR NOT NULL);
             """,
    )

    truncate_table = PostgresOperator(
        task_id='truncate_table',
        postgres_conn_id=CONN_ID,
        sql=f"TRUNCATE TABLE {TABLE_NAME};",
        autocommit=True,
    )

    insert_date = PostgresOperator(
        task_id="load_date",
        postgres_conn_id=CONN_ID,
        sql=F"""
               INSERT INTO {TABLE_NAME} (name, pet_type, birth_date, OWNER)
               VALUES ( 'Max', 'Dog', '2018-07-05', 'Jane');
               INSERT INTO {TABLE_NAME} (name, pet_type, birth_date, OWNER)
               VALUES ( 'Susie', 'Cat', '2019-05-01', 'Phil');
               INSERT INTO {TABLE_NAME} (name, pet_type, birth_date, OWNER)
               VALUES ( 'Lester', 'Hamster', '2020-06-23', 'Lily');
               INSERT INTO {TABLE_NAME} (name, pet_type, birth_date, OWNER)
               VALUES ( 'Quincy', 'Parrot', '2013-08-11', 'Anne');
               """,
    )

    select_date = PostgresOperator(
        task_id="select_date",
        sql=f"SELECT * FROM {TABLE_NAME};",
        postgres_conn_id=CONN_ID
    )

    create_table >> truncate_table >> insert_date >> select_date

