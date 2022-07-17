"""
Даг по заданию 2.
Работает с понедельника по субботу
Забирает данные из GreenPlum
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import datetime
import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'e-bagadaeva-4',
    'poke_interval': 600
}

dag = DAG("e-bagadaeva-4-greenplum",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['e-bagadaeva-4']
          )


def load_data(date: str):
    date_list = date.split('-')
    weekday = datetime.datetime(int(date_list[0]), int(date_list[1]), int(date_list[2])).weekday() + 1
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor("e-bagadaeva-4_cursor")
    cursor.execute(f'SELECT heading FROM articles WHERE id={weekday}')
    result = cursor.fetchone()[0]
    logging.info("result")
    return result


read_greenplum = PythonOperator(
    task_id='read_greenplum',
    python_callable=load_data,
    dag=dag,
    op_args=["{{a-gajdabura}}"]
)

read_greenplum

dag.doc_md = __doc__

read_greenplum.doc_md = """Забирает данные с GreenPlum"""