"""
Загружаем данные из API Рика и Морти
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator

from j_botnikov_plugins.y_botnikov_locations_operator import Top3Locations

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'Botnikov',
    'poke_interval': 600
}


with DAG("yury_ram_locations",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['botnikov', 'hw3']
         ) as dag:

    start = DummyOperator(task_id='start')

    get_top_locations = Top3Locations(
        task_id='Top3Locations',
        top=3
    )

    start >> get_top_locations
