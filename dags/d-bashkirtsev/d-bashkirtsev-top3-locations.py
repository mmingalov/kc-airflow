"""
DAG по заданию №3 блока ETL

Используя API RaM вычисляем три локации с наибольшим количеством резидентов.
Запись производится в GP
"""

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from d_bashkirtsev_plugins.d_bashkirtsev_ram_locations_op import d_bashkirtsev_ram_locations_op

DEFAULT_ARGS = {
    'start_date': days_ago(0),
    'owner': 'd-bashkirtsev',
    'poke_interval': 600
}
TABLE_NAME = 'd_bashkirtsev_ram_location'

with DAG("d_bashkirtsev_load_ram_loc_cnt",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['bashkirtsev', 'lesson5']
         ) as dag:
    start = DummyOperator(task_id='start')

    create_table = PostgresOperator(
        task_id='create_table',
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id  SERIAL4 PRIMARY KEY,
                name VARCHAR NOT NULL,
                type VARCHAR NOT NULL,
                dimension VARCHAR NOT NULL,
                resident_cnt INT4 NOT NULL);
        """,
        autocommit=True,
        postgres_conn_id='conn_greenplum_write',
        dag=dag
    )
    truncate_table = PostgresOperator(
        task_id='truncate_table',
        sql=f"""
                                            TRUNCATE TABLE {TABLE_NAME}
                                        """,
        autocommit=True,
        postgres_conn_id='conn_greenplum_write',
        dag=dag
    )

    get_top3_loc = d_bashkirtsev_ram_locations_op(
        task_id='get_top3_loc'
    )

    end = DummyOperator(task_id='end')

    start >> create_table >> truncate_table >> get_top3_loc >> end
