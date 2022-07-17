"""
    Tasks:
    1) Создайте в GreenPlum'е таблицу с названием "<ваш_логин>_ram_location"
    с полями id, name, type, dimension, resident_cnt.
    2) С помощью API (https://rickandmortyapi.com/documentation/#location)
    найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
    3) Запишите значения соответствующих полей этих трёх локаций в таблицу.
    resident_cnt — длина списка в поле residents.
"""

from airflow import DAG
from datetime import timedelta
import logging
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from plugins.s_evsyukov_plugins.s_evsyukov_operator import *

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-evsjukov',
    'poke_interval': 300,
}


with DAG(
        dag_id="s-evsjukov-ram-gad7"
               "",
        schedule_interval='@once',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['s-evsjukov']
) as dag:

    # DB name
    students_db = 'students.public.s_evsjukov_ram_location'

    # URL to download data
    url = 'https://rickandmortyapi.com/api/location'

    # initiate_gp = SevyukovInitiateGP(
    #     task_id='initiate_gp',
    #     db_name=students_db
    # )

    dummy_operator1 = DummyOperator(
        task_id='dummy_operator'
    )

    def hello_world_func():
        logging.info("Hello World!")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )

    # load_data = SevsyukovLoadFromURLOperator(
    #     task_id='load_data',
    #     url=url,
    #     provide_context=True
    # )
    #
    # insert_data = SevsyukovQueryOperator(
    #     task_id='insert_data',
    #     db_name=students_db
    # )

    dummy_operator1 >> hello_world

