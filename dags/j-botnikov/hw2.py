from airflow import DAG
import logging
from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

DEFAULT_ARGS = {
    'owner': 'Y.Botnikov',
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'catchup': True
}


with DAG(
    "hw2_dag",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['hw2', 'botnikov']
) as dag:

    def weekday_check_fn(ds):
        weekday = datetime.strptime(ds, '%Y-%m-%d').weekday()
        logging.info(f"Date is {ds}. Weekday is {weekday}")
        return weekday != 6


    weekday_check = ShortCircuitOperator(
        task_id='weekday_check',
        python_callable=weekday_check_fn,
        op_kwargs={'ds': '{{ ds }}'}
    )


    def read_headings(ds):
        weekday = datetime.strptime(ds, '%Y-%m-%d').weekday() + 1
        logging.info(f'Date is {ds}. Weekday is {weekday}')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')
        try:
            heading = cursor.fetchone()[0]
        except:
            heading = 'пусто'
        logging.info(heading)

    read_headings = PythonOperator(
        task_id='read_headings',
        python_callable=read_headings,
        op_kwargs={'ds': '{{ tomorrow_ds }}'}
    )


    weekday_check >> read_headings
