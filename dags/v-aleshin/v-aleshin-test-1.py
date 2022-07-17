"""
Test dag
"""

from airflow import DAG

import logging
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 5, 1),
    'end_date': datetime(2022, 5, 14),
    'owner': 'v-aleshin'
}


def get_articles_from_gp():
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f'SELECT heading FROM articles WHERE id = {datetime.now().weekday() + 1}')
    heading = cursor.fetchone()[0]
    logging.info(heading)


with DAG(
        dag_id='valeshin_test_dag_1',
        schedule_interval='0 0 * * 1-6',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['valeshin'],
        catchup=True
) as dag:

    get_articles_from_gp = PythonOperator(
        task_id='get_articles_from_gp',
        python_callable=get_articles_from_gp
    )

    get_articles_from_gp

