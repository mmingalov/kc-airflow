"""
Lesson 4 section 2 -- ETL
Dag works from 03/01/2022 to 03/14/2022
in all days of week except Sunday

Dag connects to Greenplum and gets first row from articles table
(heading column) where id equal to dag execution weekday number

Result of query logged
"""

import logging
from airflow import DAG
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022,3,1),
    'end_date': datetime(2022,3,14),
    'owner': 'ta-didova',
    'poke_interval': 600
}

with DAG("ta_didova_GP_lesson4",
          schedule_interval='00 5 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['ta_didova']
    ) as dag:

    dummy = DummyOperator(task_id='dummy')

    def get_greenplum_article(id):
        logging.info("Start loading article")
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("gp_conn")
        cursor.execute('SELECT heading FROM articles WHERE id = {};'.format(id))
        return logging.info(cursor.fetchone()[0])

    get_article_title = PythonOperator(
        task_id='get_article_title',
        python_callable=get_greenplum_article,
        op_args=['{{dag_run.logical_date.weekday() + 1}}'],
        dag=dag
    )

    dummy >> get_article_title
