"""
Создает в GreenPlum'е таблицу с названием "v-novokreschenov-8_ram_location"
с полями id, name, type, dimension, resident_cnt.
С помощью API (https://rickandmortyapi.com/documentation/#location)
находит три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
Записывает значения соответствующих полей этих трёх локаций в таблицу.
resident_cnt — длина списка в поле residents.
"""

from airflow import DAG
import logging

from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from v_novokreschenov_8_plugins.v_novokreschenov_8_ram_operator import (
    VNovokreschenov8Operator
)


DEFAULT_ARGS = {
    'owner': 'v-novokreschenov-8',
    'start_date': days_ago(2),
    'poke_interval': 600,
}

with DAG(
    "v-novokreschenov-8-5",
    default_args=DEFAULT_ARGS,
    schedule_interval='@once',
    max_active_runs=1,
    tags=['v-novokreschenov-8-5']
) as dag:

    start = DummyOperator(task_id="start")

    extract_data_locations = VNovokreschenov8Operator(
        task_id='extract_data_locations',
        do_xcom_push=True,
        dag=dag
    )

    def transform_data(task_instance):
        data = task_instance.xcom_pull(
            task_ids='extract_data_locations',
            dag_id='v-novokreschenov-8-5',
            key='return_value'
        )
        logging.info(f"data_locations: {str(data)}")
        data_sort = sorted(data, key=lambda x: x[4], reverse=True)
        top3 = data_sort[:3]
        logging.info(f"data_locations_sort: {str(top3)}")
        return top3

    transform_data_locations = PythonOperator(
        task_id='transform_data_locations',
        python_callable=transform_data,
        do_xcom_push=True,
        dag=dag
    )

    def load_data(task_instance):
        data_locations = task_instance.xcom_pull(
            task_ids='transform_data_locations',
            key='return_value'
        )
        logging.info("start loading data locations")
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        create_table = """
        CREATE TABLE IF NOT EXISTS vnovokreschenov_ram_location (
           id INT PRIMARY KEY,
           name text,
           type text,
           dimension text,
           resident_cnt INT NOT NULL
        );
        TRUNCATE vnovokreschenov_ram_location;
        """
        pg_hook.run(create_table, False)
        pg_hook.insert_rows(
            table='public."vnovokreschenov_ram_location"',
            rows=data_locations,
            target_fields=['id', 'name', 'type', 'dimension', 'resident_cnt']
        )

    load_data_locations = PythonOperator(
        task_id='load_data_locations',
        python_callable=load_data,
        dag=dag
    )

    end = DummyOperator(task_id="end")

    (
        start >>
        extract_data_locations >>
        transform_data_locations >>
        load_data_locations >>
        end
    )


dag.doc_md = __doc__

start.doc_md = """Начинает DAG"""
extract_data_locations.doc_md = """Выгружает данные RAM"""
transform_data_locations.doc_md = """Преобразует данные RAM"""
load_data_locations.doc_md = """Сохраняет данные RAM"""
end.doc_md = """Завершает DAG"""
