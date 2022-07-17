from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from a_tskhay_plugins.a_tskhay_ram_location_plugin import ATskhayLocationOperator
import logging

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-tskhay'
}

def truncate_func():
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('truncate a_tskhay_ram_location')
    conn.commit()
    conn.close()

def load_to_gp_func(ti):
    df = ti.xcom_pull(key='return_value', task_ids='get_top_locations')
    logging.info('This is df from XCom')
    logging.info(df)

    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    rows_to_add = list(df.itertuples(index=False, name=None))
    logging.info('These are rows to add in GP')
    logging.info(rows_to_add)

    pg_hook.insert_rows(table='a_tskhay_ram_location', rows=rows_to_add)

with DAG("a_tskhay_ram_location_DAG",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         tags=['a-tskhay']
         ) as dag:

    start = DummyOperator(task_id='start')

    truncate_table = PythonOperator(
        task_id='truncate_table',
        python_callable=truncate_func
    )

    get_top_locations = ATskhayLocationOperator(
        task_id='get_top_locations'
    )

    load_to_gp = PythonOperator(
        task_id='load_to_gp',
        python_callable=load_to_gp_func
    )

    start >> truncate_table >> get_top_locations >> load_to_gp