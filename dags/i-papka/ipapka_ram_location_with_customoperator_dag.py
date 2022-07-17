"""
Загружаем данные из API Рика и Морти в GreenPlum
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.postgres_operator import PostgresOperator
from ipapka_plugins.ipapka_ram_location_count_operator import IpapkaRamLocationCountOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'i-papka',
    'poke_interval': 600
}


with DAG("ipapka_ram_location_with_customoperator_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['i-papka']
         ) as dag:

    create_table_sql_query = """ 
                    CREATE TABLE IF NOT EXISTS "i-papka_ram_location" (
                    id serial4,
                    name varchar,
                    type varchar,
                    dimension varchar,
                    resident_cnt int4);
                    TRUNCATE TABLE  "i-papka_ram_location";
                """

    create_table = PostgresOperator(
        sql=create_table_sql_query,
        task_id="create_table",
        postgres_conn_id="conn_greenplum_write",
        dag=dag
    )

    insert_values = IpapkaRamLocationCountOperator(
        task_id='insert_values',
        dag=dag
    )

    create_table >> insert_values
