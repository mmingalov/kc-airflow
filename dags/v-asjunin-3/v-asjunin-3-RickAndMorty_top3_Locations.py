
"""
Создайте в GreenPlum'е таблицу с названием "<ваш_логин>_ram_location" с полями id, name, type, dimension, resident_cnt.
С помощью API (https://rickandmortyapi.com/documentation/#location) найдите три локации сериала "Рик и Морти"
с наибольшим количеством резидентов.
Запишите значения соответствующих полей этих трёх локаций в таблицу. resident_cnt — длина списка в поле residents
"""

import datetime
import logging

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from v_asjunin_plugins.v_asjunin_RAM import RamTop3LocationOperator

default_args = {
    'start_date': days_ago(0),
    'owner': 'Slava',
    'poke_interval': 600
}

dag = DAG('v-asjunin-3_RAM_Top3_Locations_dag',
    default_args=default_args,
    schedule_interval='5 0 * * 1-6',
    max_active_runs=1,
    tags=['v-asjunin']
)

poehali = DummyOperator(task_id='Poehali', dag=dag)

Get_Top3_RAM_locations = RamTop3LocationOperator(
    task_id='Get_Top3_RAM_locations',
    dag=dag
)

def getLocations(**kwargs):

    locations = kwargs['ti'].xcom_pull(task_ids='Get_Top3_RAM_locations', key='RickAndMorty_Top3_Location')

    query = "delete from students.public.v_asjunin_3_ram_location; " + \
            "INSERT INTO students.public.v_asjunin_3_ram_location(id, name, type, dimension, resident_cnt) values "
    id = 0
    values = []

    for loc in locations:
        id += 1
        values.append(f"({id}, '{loc['name']}', '{loc['type']}', '{loc['dimension']}', {loc['resident_cnt']})")

    query = query + ", ".join(values) + ";"

    logging.info("LOGGING: query=" + query)

    hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()


Load_Locations_To_GreenPlum = PythonOperator(
    task_id='Load_Locations_To_GreenPlum',
    python_callable=getLocations,
    provide_context=True,
    dag=dag
)

poehali >> Get_Top3_RAM_locations >> Load_Locations_To_GreenPlum
