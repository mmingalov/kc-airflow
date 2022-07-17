"""
DAG для ДЗ 2
"""

from airflow import DAG
from datetime import datetime
import logging

from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'n-surov',
    'poke_interval': 600
}

with DAG("n-surov_2",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['n-surov']
          ) as dag:

    def gp_to_xcom(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        exec_date_to_id = datetime.strptime(kwargs.get('a-gajdabura'), '%Y-%m-%d').weekday() + 1
        query = 'SELECT heading FROM articles WHERE id = {id_}'.format(id_=exec_date_to_id)
        logging.info(query)
        cursor.execute(query)
        one_string = cursor.fetchone()[0]
        logging.info('PUT in XCOM: {one_string_}'.format(one_string_=one_string))
        kwargs['ti'].xcom_push(value=one_string, key='return_val_gp')

    dummy = DummyOperator(task_id="dummy")

    gp_to_xcom = PythonOperator(
        task_id='gp_to_xcom',
        python_callable=gp_to_xcom,
        dag=dag
    )

    dummy >> gp_to_xcom

dag.doc_md = __doc__

gp_to_xcom.doc_md = """Привет"""