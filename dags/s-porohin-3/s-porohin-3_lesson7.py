"""
ETL урок 7
Создайте в GreenPlum'е таблицу с названием "<ваш_логин>_ram_location" с полями id, name, type, dimension, resident_cnt.
С помощью API rickandmortyapi.com найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
Запишите значения соответствующих полей этих трёх локаций в таблицу. resident_cnt — длина списка в поле residents.
Что-бы не создавать бесполезную нагрузку на api, оставил только ручной запуск
"""
import logging
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from s_porohin_3_plugins.s_porohin_3_RaM_location import SPorohin3RaMLocationResidentTopOperator


DEFAULT_ARGS = {
    'start_date': days_ago(0),
    'owner': 's-porohin-3',
    'poke_interval': 600
}

with DAG("s-porohin-3_lesson7",
         schedule_interval=None,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['s-porohin-3']
         ) as dag:

    ram_parse = SPorohin3RaMLocationResidentTopOperator(
        task_id='ram_top_location_residents_get',
        resident_top_count=3
    )

    def save_data_to_greenplum(task_instance):
        rows = task_instance.xcom_pull(
            task_ids='ram_top_location_residents_get',
            key='return_value'
        )
        logging.info(f"Rows from XCOM: {str(rows)}")
        if not rows:
            raise AirflowException('Invalid XCOM data')
        rows_values = [[r['id'], r['name'], r['type'], r['dimension'], r['resident_cnt']] for r in rows]
        rows_values = [item for sublist in rows_values for item in sublist]
        logging.info(f"Values for insert: {str(rows_values)}")
        table_name = 's_porohin_3_ram_location'
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
               id INT PRIMARY KEY,
               name text,
               type text,
               dimension text,
               resident_cnt INT NOT NULL
            );
            TRUNCATE {table_name};
            INSERT INTO {table_name} (
                id,
                name,
                type,
                dimension,
                resident_cnt
            ) VALUES {','.join(["(%s,%s,%s,%s,%s)"]*len(rows))} ;
            """, rows_values )
        conn.commit()
        cursor.close()
        conn.close()

    save_data = PythonOperator(
        task_id='save_data_to_greenplum',
        python_callable=save_data_to_greenplum
    )

    ram_parse >> save_data
