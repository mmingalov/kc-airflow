"""
Lesson4. Get from articles headings, where id == weekday
"""
import logging

from airflow import DAG
import pendulum
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022,3,1),
    'end_date': pendulum.datetime(2022,3,15),
    'owner': 'k.chudakova-13',
    'catchup': True,
    'poke_interval': 600
}

with DAG("k.chudakova-13_les4_log_info",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['k.chudakova-13']
) as dag:

    def get_data_from_greenplum(week_day):
        # Connection to Greenplum
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        week_day = int(week_day) + 1

        # Get data from database
        cursor.execute(f'SELECT heading FROM articles WHERE id = {week_day}')
        heading_string = cursor.fetchall()

        # Logging result
        logging.info('--------')
        logging.info(f'Weekday: {week_day}')
        logging.info(f'Return value: {heading_string}')
        logging.info('--------')


    get_data_and_log_info = PythonOperator(
        task_id='get_data_and_log_info',
        python_callable=get_data_from_greenplum,
        op_args=["{{ execution_date.weekday() }}"]
    )

get_data_and_log_info