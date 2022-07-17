from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
import logging
from datetime import datetime
import pandas as pd

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 's.kitaev-2',
    'poke_interval': 600
}

with DAG("s.kitaev-2_second_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['s.kitaev-2']
         ) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    def hello_world():
        logging.info('Hello, World!')


    def get_heading_by_week_day(**kwargs):
        today = datetime.today().weekday() + 1
        logging.info(f'Current week day: {today}')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        pg_conn = pg_hook.get_conn()
        heading_df = pd.read_sql(f'SELECT heading FROM articles WHERE id = {today}', con=pg_conn)
        logging.info(f'Debug: {heading_df}')
        if heading_df.shape != (0, 0):
            query_res = heading_df['heading'].values[0]
            kwargs['task_instance'].xcom_push(key='s-kitaev-2_xcom_push', value=query_res)
        else:
            logging.info(f'Empty query received')


    step = PythonOperator(
        task_id='step',
        python_callable=get_heading_by_week_day
    )


    start >> step >> end
