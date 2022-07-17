"""
Работать с понедельника по субботу, но не по воскресеньям
Ходить в наш GreenPlum.
Забирать из таблицы articles значение поля heading из строки с id, равным дню недели
Выводить результат работы в логах
Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года
"""
from airflow import DAG
import logging
import pendulum

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1),
    'end_date': pendulum.datetime(2022, 3, 14),
    'owner': 'v-nemchenko-8',
    'poke_interval': 600
}

with DAG("v-nemchenko-8_second_dag",
            schedule_interval='0 0 * *  1-6',
            default_args=DEFAULT_ARGS,
            max_active_runs=1,
            tags=['v-nemchenko-8']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    def get_heading(week_day):
        week_day = int(week_day) + 1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {week_day}')
        query_res = cursor.fetchall()
        logging.info(f"REQUEST: SELECT heading FROM articles WHERE id = {week_day}\nRESULTS:\n{query_res} ")

    python_get_heading = PythonOperator(
            task_id='python_get_heading',
            python_callable=get_heading,
            dag=dag,
            op_args=["{{ execution_date.weekday() }}"]
    )


    dummy >> python_get_heading