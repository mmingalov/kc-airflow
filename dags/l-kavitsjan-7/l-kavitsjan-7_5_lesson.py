"""
Домашнее задание. Top 3 locations Урок 5
Done
"""

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from l_kavitsjan_7_plugins.l_kavitsjan_7_ram_operator import KavitsjanRamLocationCountOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'l-kavitsjan-7',
}

TABLE_NAME = 'l_kavitsjan_7_ram_location'
CONN_ID = 'conn_greenplum_write'

dag = DAG("l-kavitsijan-7_5_lesson",
          schedule_interval='0 0 * * *',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['l-kavitsjan-7']
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
    sql="""
        CREATE TABLE IF NOT EXISTS {table} (
            id  SERIAL4 PRIMARY KEY,
            name VARCHAR NOT NULL,
            type VARCHAR NOT NULL,
            dimension VARCHAR NOT NULL,
            resident_cnt INT4 NOT NULL);
    """.format(table=TABLE_NAME),
    autocommit=True,
    postgres_conn_id=CONN_ID,
    dag=dag
)

truncate_table = PostgresOperator(
    task_id='truncate_table',
    postgres_conn_id=CONN_ID,
    sql="TRUNCATE TABLE {table};".format(table=TABLE_NAME),
    autocommit=True,
    dag=dag
)

load_ram_top_locations = KavitsjanRamLocationCountOperator(
    task_id='load_ram_top_locations',
    top_n=3,
    table_name=TABLE_NAME,
    conn_id=CONN_ID,
    dag=dag
)


dummy_op_start >> create_table >> truncate_table >> load_ram_top_locations >> dummy_op_end