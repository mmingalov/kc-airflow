"""
Обновляет данные таблицы o_voinova_7_ram_location из источника https://rickandmortyapi.com
"""

from airflow import DAG
from datetime import datetime

from airflow.providers.postgres.operators.postgres import PostgresOperator
from o_voinova_7_plugins.o_voinova_7_ram_operator import OVoinova7RickMortyOperator


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'o_voinova_7',
    'tags': ['o-voinova-7'],
}


with DAG("o-voinova-7_plugins",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          catchup=True,
         ) as dag:

    insert_data = OVoinova7RickMortyOperator(
        task_id='insert_data',
        top_cnt=3,
    )
    truncate = PostgresOperator(
        task_id='truncate',
        postgres_conn_id='conn_greenplum_write',
        sql=f"truncate table o_voinova_7_ram_location",
    )

truncate >> insert_data
