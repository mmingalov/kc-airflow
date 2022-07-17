"""
Загрузка статьи из GP
"""
from datetime import datetime
from airflow import DAG
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1, 17, 5),
    'owner': 'n_paveleva',
    'poke_interval': 600
}

with DAG("n_paveleva_articles_daily",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['n_paveleva']
         ) as dag:
    start_task = DummyOperator(task_id='start')


    def get_article(week_day, **kwargs):
        pg_hook = PostgresHook('conn_greenplum')
        logging.info("Getting text of the article '{}'" \
                     .format(datetime.strptime(week_day, "%Y-%m-%d").date().isoweekday()))
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT heading FROM articles WHERE id = 5')
        query_res = cursor.fetchone()[0]
        conn.close()
        kwargs['ti'].xcom_push(value=query_res, key='heading')


    get_text = PythonOperator(
        task_id='get_article',
        op_kwargs={'week_day': "{{ ds }}"},
        python_callable=get_article,
        provide_context=True
    )

    start_task >> get_text
