"""
Задание из урока 4
"""

from airflow import DAG
import logging

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pendulum


DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz="UTC"),
    'end_date': pendulum.datetime(2022, 3, 14, tz="UTC"), #Запускаем даг только для заданного периода
    'owner': 's-chertkov',
    'poke_interval': 600
}

with DAG("s-chertkov-task-4",
         schedule_interval='0 0 * * 1-6',  # Запускаем с понедельника по субботу в полночь
         catchup=True,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['s-chertkov']
         ) as dag:

    def query_greenplum(weekday):
        logging.info(f"Running query for date - in {weekday}")

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("my-task-cursor")
        cursor.execute(f"SELECT heading FROM articles WHERE id = {weekday}")
        response = cursor.fetchall()
        logging.info(response)
        return response

    query_data = PythonOperator(
        task_id='query-postgres',
        python_callable=query_greenplum,
        op_args=['{{ data_interval_start.weekday() + 1 }}'],
        do_xcom_push=True,
    )


