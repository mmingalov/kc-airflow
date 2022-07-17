"""
Определяем три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
"""

import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

from a_gujdo_4_plugins.a_gujdo_4_ram_top_operator import GualexRamTopLocationOperator

DEFAULT_ARGS = {
    'owner': 'a-gujdo-4',
    'start_date': days_ago(1),
}


def save_result(**kwargs):
    ti = kwargs['ti']
    top = ti.xcom_pull(task_ids='get_top_location', key='top')

    logging.info('TOP')
    logging.info(top)

    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

    sql_statement = 'truncate a_gujdo_4_ram_location'
    pg_hook.run(sql_statement, False)

    top_list = [(item['id'], item['name'], item['type'], item['dimension'], item['resident_cnt'])
                for item in top]

    pg_hook.insert_rows(
        table='a_gujdo_4_ram_location',
        rows=top_list,
        target_fields=['id', 'name', 'type', 'dimension', 'resident_cnt']
    )


with DAG("a-gujdo-4_les5_ram",
         schedule_interval='@once',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-gujdo-4']
         ) as dag:

    top3_operator = GualexRamTopLocationOperator(
        task_id='get_top_location',
        top_len=3
    )

    save_result_operator = PythonOperator(
        task_id='save_result',
        python_callable=save_result
    )

    top3_operator >> save_result_operator
