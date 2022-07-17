import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from p_evtuhovich_8_plugins.p_evtuhovich_8_ram_residents import PavelEvtuhovichRamLocationOperator

# from airflow.providers.postgres.hook.postgres import PostgresHook
# from airflow.providers.postgres.operators.postgres import PostgresOperator

DEFAULT_ARGS = {
    'start_date':        days_ago(0),
    'owner':             'p-evtuhovich-8',
    'e-mail':            'p.evtuhovich.pos.credit@gmail.com'
}


dag = DAG(
    'p_evtuhovich_8_dag_ram_location',
    default_args=DEFAULT_ARGS,
    schedule_interval='@hourly',
    max_active_runs=1,
    tags=['p-evtuhovich-8']
)


create_table = PostgresOperator(
    postgres_conn_id='conn_greenplum_write',
    task_id='create_table',
    sql="""
    CREATE TABLE IF NOT EXISTS 
    public.p_evtuhovich_8_ram_location (
        id              INT PRIMARY KEY,
        name            VARCHAR NOT NULL,
        type            VARCHAR NOT NULL,
        dimension       VARCHAR NOT NULL,
        resident_cnt    INT NOT NULL
        );
    """,
    autocommit=True,
    dag=dag
)


delete_data = PostgresOperator(
    postgres_conn_id='conn_greenplum_write',
    task_id='delete_data',
    sql="""
    DELETE FROM p_evtuhovich_8_ram_location;
    """,
    autocommit=True,
    dag=dag
)


get_list_to_record = PavelEvtuhovichRamLocationOperator(
    task_id='get_list_to_record',
    dag=dag
)


def load_to_db(ti):
    result_location_info = ti.xcom_pull(key='return_value', task_ids='get_list_to_record')
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    with conn.cursor() as cursor:
        query = """
                INSERT INTO public.p_evtuhovich_8_ram_location (id, name, type, dimension, resident_cnt)
                VALUES (%s, %s, %s, %s, %s)
                """
        cursor.executemany(query, result_location_info)
        logging.info(query)
        conn.commit()
        cursor.close()


load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_to_db,
    dag=dag
)


create_table >> delete_data >> get_list_to_record >> load_data
