"""
Тренировочный даг.

Cостоит из двух операторов:
первый - самописный оператор, забирающий по API данные по локациям и резидентам и выбирающий топ-3
по количеству резидентов,
второй - питон-оператор, сохраняющий топ-3 в таблицу БД.
"""
import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from ndavydova_plugins.ndavydova_residents_count_operator import (
    DavydovaLocationsWithMaxResidentsOperator
)
from sqlalchemy import create_engine

TABLE_NAME = 'n_davydova_2_ram_location'
DEF_CONN_ID = 'conn_greenplum_write'

DEFAULT_ARGS = {
    'start_date': dt.datetime(2021, 11, 20),
    'owner': 'NDavydova',
    'retries': 3
}

with DAG(
    "ndavydova_ram_dag",
    schedule_interval='@once',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['karpov_courses'],
) as dag:

    get_top3 = DavydovaLocationsWithMaxResidentsOperator(
        task_id='get_top3',
        url='https://rickandmortyapi.com/api/location'
    )

    def write_top3_f(**context):
        top_df = context['ti'].xcom_pull(key='davydova_ram')
        pg_hook = PostgresHook(postgres_conn_id=DEF_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        top_df.to_sql(name=TABLE_NAME, con=engine, if_exists='replace')  # creates table, if id doesn't exist
        engine.dispose()

    write_to_db = PythonOperator(
        task_id='write_to_db',
        python_callable=write_top3_f,
    )

get_top3 >> write_to_db

dag.doc_md = __doc__
get_top3.doc_md = """Gets top-3 locations with residents"""
write_to_db.md = """Puts top-3 locations to db"""

