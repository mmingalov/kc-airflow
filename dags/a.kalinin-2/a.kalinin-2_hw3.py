"""
Домашнее задание. Top 3 locations
"""

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from kalinin_plugins.kalinin_ram_locations_operator import KalininTopLocationCountOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a.kalinin-2'
}

TABLE_NAME = 'kalinin_2_ram_location'
CONN_ID = 'conn_greenplum_write'

dag = DAG("a.kalinin-2_third_dag",
          schedule_interval='0 0 * * *',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a.kalinin-2']
          )

dummy_op_start = DummyOperator(
    task_id='start_task',
    dag=dag,
  )

dummy_op_end = DummyOperator(
    task_id='end_task',
    dag=dag,
  )


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
    postgres_conn_id=CONN_ID,
    dag=dag
)

truncate_table = PostgresOperator(
    task_id='truncate_table',
    postgres_conn_id=CONN_ID,
    sql=f"TRUNCATE TABLE {TABLE_NAME};",
    autocommit=True,
    dag=dag
)

load_ram_top_locations = KalininTopLocationCountOperator(
    task_id='load_ram_top_locations',
    top_n=3,
    table_name=TABLE_NAME,
    conn_id=CONN_ID,
    dag=dag
)


dummy_op_start >> create_table >> truncate_table >> load_ram_top_locations >> dummy_op_end