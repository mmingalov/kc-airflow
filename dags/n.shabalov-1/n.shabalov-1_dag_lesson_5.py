from airflow.utils.dates import days_ago
import pandas as pd
from sqlalchemy import create_engine
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

from shabalov_plugins.shabalov_location_operator import ShabalovLocationOperator

TABLE_NAME = 'shabalov_location_top3'
GREENPULM_CONN = 'conn_greenplum_write'

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'n.shabalov-1',
    'poke_interval': 600
}

def write_to_greenpulm(**kwargs):
    info = kwargs['ti'].xcom_pull(task_ids='get_residents_cnt')
    df = pd.DataFrame(info, columns=['id', 'name', 'type', 'dimension', 'resident_cnt']).sort_values(
        by='resident_cnt', ascending=False).head(3)
    logging.info("Информация получена")
    postgres_hook = PostgresHook(GREENPULM_CONN)
    engine = postgres_hook.get_sqlalchemy_engine()
    logging.info("Соединение установленно")
    df.to_sql(name=TABLE_NAME, con=engine,
              if_exists='replace', index=False)
    engine.dispose()
    logging.info("Таблица успешно записана")

dag = DAG("n.shabalov-1_dag_lesson_5",
          schedule_interval=None,
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['karpov', 'nshabalov']
          )

create_table = PostgresOperator(
        task_id='create_table',
        sql=f'''
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id INTEGER NOT NULL,
                name VARCHAR NOT NULL,
                type VARCHAR NOT NULL,
                dimension VARCHAR NOT NULL,
                resident_cnt INTEGER NOT NULL,
                PRIMARY KEY (id)
                );
            ''',
        postgres_conn_id=GREENPULM_CONN,
        dag=dag
    )

get_locations_info = ShabalovLocationOperator(
        task_id='get_residents_cnt',
        do_xcom_push=True,
        dag=dag
    )

top3_write_to_greenpulm = PythonOperator(
    task_id='script_task',
    python_callable=write_to_greenpulm,
    dag=dag)

create_table >> get_locations_info >> top3_write_to_greenpulm