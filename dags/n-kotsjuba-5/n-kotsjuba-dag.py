"""
Даг получает название статьи с id = день недели
Работает с понедельника по суббботу

"""

from airflow import DAG

from airflow.utils.dates import days_ago
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'n-kotsjuba-5',
    'poke_interval': 600
}

with DAG("n-kotsjuba-dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['n-kotsjuba-5']
          ) as dag:

    def day_of_week(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day

    def is_not_sunday_func(execution_dt):
        exec_day = day_of_week(execution_dt)
        return exec_day in [0, 1, 2, 3, 4, 5]

    not_sunday = ShortCircuitOperator(
        task_id='not_sunday',
        python_callable=is_not_sunday_func,
        op_kwargs={'execution_dt': '{{ a-gajdabura }}'}
    )

    def get_article_heading_from_gp_func(execution_dt, **kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        exec_day = day_of_week(execution_dt)
        cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(exec_day))
        one_string = cursor.fetchone()[0]
        kwargs['ti'].xcom_push(key='article_name', value=one_string)

    get_article_heading_from_gp = PythonOperator(
        task_id='get_article_heading_from_gp',
        python_callable=get_article_heading_from_gp_func,
        op_kwargs={'execution_dt': '{{ a-gajdabura }}'},
        dag=dag
    )

    not_sunday >> get_article_heading_from_gp
