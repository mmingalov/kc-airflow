"""
Создайте в GreenPlum'е таблицу с названием "<ваш_логин>_ram_location" с полями id, name, type, dimension, resident_cnt.
С помощью API (https://rickandmortyapi.com/documentation/#location) найдите три локации сериала "Рик и Морти"
с наибольшим количеством резидентов.
Запишите значения соответствующих полей этих трёх локаций в таблицу. resident_cnt — длина списка в поле residents.
"""

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from m_mingalov_plugins.rick_and_morty_operator import TopLocationsRickAndMorty

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'm-mingalov',
    'poke_interval': 606
}

with DAG("m-mingalov_5_Rick_and_Morty",
         schedule_interval=None,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['m-mingalov']
         ) as dag:

    start_process = DummyOperator(task_id='start_process')

    create_or_truncate_table = PostgresOperator(
        task_id='create_or_truncate_table',
        postgres_conn_id='conn_greenplum_write',
        sql=[
            '''
            CREATE TABLE IF NOT EXISTS "m-mingalov_ram_location"
            (
                id           INTEGER PRIMARY KEY,
                name         VARCHAR(256),
                type         VARCHAR(256),
                dimension    VARCHAR(256),
                resident_cnt INTEGER
            )
                DISTRIBUTED BY (id);''',
            '''TRUNCATE TABLE "m-mingalov_ram_location";'''
        ],
        autocommit=True
    )

    top_locations_rick_and_morty = TopLocationsRickAndMorty(task_id='top_locations_rick_and_morty')

    end_process = DummyOperator(task_id='end_process')
    #DAG
    start_process >> create_or_truncate_table >> top_locations_rick_and_morty >> end_process