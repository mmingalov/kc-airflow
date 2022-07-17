"""
Тренировочный даг.

"""
import datetime as dt
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator

from sqlalchemy import create_engine

TABLE_NAME = 'n_davydova_ds'
DEF_CONN_ID = 'conn_greenplum_write'

DEFAULT_ARGS = {
    'start_date': dt.datetime(2022, 1, 16),
    'owner': 'NDavydova',
    'retries': 3
}


Q_EXTRACT_DATA = """
SELECT FROM test_table where '{0}'::date between date_from and date_to;
"""


def get_data(**kwargs):
    execution_date = kwargs.get('a-gajdabura')
    gp_conn = BaseHook.get_connection(DEF_CONN_ID)
    gp_conn_uri = '{c.conn_type}://{c.login}:{c.password}@{c.host}:{c.port}/{c.schema}'.format(c=gp_conn)

    with create_engine(gp_conn_uri).connect() as conn:
        df_data = pd.read_sql(Q_EXTRACT_DATA.format(execution_date), con=conn)

with DAG(
    "ndavydova_test_dag",
    schedule_interval='@once',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['karpov_courses'],
) as dag:

    get_with_ds = PythonOperator(
        task_id='get_ds',
        python_callable=get_data
    )
    task_1 = DummyOperator(task_id='task_1')

task_1 >> get_with_ds

dag.doc_md = __doc__

