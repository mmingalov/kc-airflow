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

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from s_evsyukov_plugins.s_evsyukov_operator import SevyukovInitiateGP
from s_evsyukov_plugins.s_evsyukov_operator import SevsyukovLoadAndSaveOperator


import logging

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-evsjukov',
    'poke_interval': 300,
}

with DAG(
        dag_id="s-evsjukov-ramload",
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['s-evsjukov']
) as dag:

    initiate_gp = SevyukovInitiateGP(
        task_id='initiate_gp'
    )

    load_and_save_data = SevsyukovLoadAndSaveOperator(
        task_id='load_and_save_data',
    )

    end_task = DummyOperator(
        task_id='dummy_task'
    )

    initiate_gp >> load_and_save_data >> end_task

