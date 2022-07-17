"""
Тестовый DAG
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


DEFAULT_ARGS = {
    'start_date': days_ago(0),
    'owner': 'a-rjapisov',
    'poke_interval': 600
}


with DAG("a-rjapisov_lab2",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-rjapisov']) as dag:

    def pg():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("articles")
        cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(datetime.today().weekday() + 1))
        result = cursor.fetchone()[0]
        return result

    python = PythonOperator(task_id='python', python_callable=pg)

    python


