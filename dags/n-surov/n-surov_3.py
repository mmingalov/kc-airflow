"""
DAG для ДЗ 3
"""

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from n_surov.n_surov_ram_location_operator import SurovLocationOperator

DEFAULT_ARGS = {
    'start_date': days_ago(0),
    'owner': 'n-surov'
}

TABLE_NAME = 'n_surov_ram_location'
CONN_ID = 'conn_greenplum_write'

dag = DAG("n-surov_3",
          schedule_interval='@once',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['n-surov']
          )

dummy_op_start = DummyOperator(
    task_id='start_task',
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
    sql=F"TRUNCATE TABLE {TABLE_NAME};",
    autocommit=True,
    dag=dag
)

load_ram_top_locations = SurovLocationOperator(
    task_id='load_ram_top_locations',
    top_n=3,
    table_name={TABLE_NAME},
    conn_id=CONN_ID,
    dag=dag
)

dummy_op_start >> create_table >> truncate_table >> load_ram_top_locations