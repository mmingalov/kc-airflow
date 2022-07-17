"""
Даг забирает данные Рик и Морти по апи
Оставляет топ3 локации по резидентам
Загружает топ3 в Greenplum

В HTTP hook используется conn_id dina_ram, т.к. для создания новых нет прав.
"""
import logging
from airflow import DAG
from airflow.utils.dates import days_ago
from a_zhukov_6_plugins.a_zhukov_ram_locations_count_operator import ZhukovRamLocationsCountOperator

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-zhukov-6',
    'poke_interval': 600
}

with DAG("a_zhukov_ram_locations_count_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         tags=['a-zhukov-6']
         ) as dag:

    top_3_locations = ZhukovRamLocationsCountOperator(
        task_id='top_3_locations',
        dag=dag
    )


    def load_locations_to_greenplum_func(**kwargs):
        data = kwargs['ti'].xcom_pull(task_ids='top_3_locations', key='return_value')
        logging.info(data)
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run("TRUNCATE a_zhukov_6_ram_location", False)
        for row in data:
            logging.info(row)
            sql = f"""INSERT INTO a_zhukov_6_ram_location (id, name, type, dimension, resident_cnt) 
                      VALUES ({row['id']}, '{row['name']}', '{row['type']}', '{row['dimension']}', {row['count_of_residents']}
                      );"""
            pg_hook.run(sql, False)


    load_locations_to_greenplum = PythonOperator(
        task_id='load_locations_to_greenplum',
        python_callable=load_locations_to_greenplum_func,
        dag=dag
    )

    top_3_locations >> load_locations_to_greenplum
