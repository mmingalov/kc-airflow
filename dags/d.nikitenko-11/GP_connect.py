"""
Dag to pull data from GP
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from airflow import macros

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'dmnikitenko',
    'poke_interval': 600
}

with DAG("d.nikitenko_gp",
         schedule_interval='00 11 * * MON-SAT',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['dmnikitenko']
         ) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    def go_to_gp(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        weekday = macros.datetime.today().weekday()
        logging.info(f'Day of the week today is number {weekday}')
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        kwargs['ti'].xcom_push(value=query_res, key=f'heading_{weekday}')

    gp_and_xcom = PythonOperator(
        task_id='gp_and_xcom',
        python_callable=go_to_gp,
        provide_context=True
    )

    start >> gp_and_xcom >> end



