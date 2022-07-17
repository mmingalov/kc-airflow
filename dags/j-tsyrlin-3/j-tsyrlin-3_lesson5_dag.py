from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
import pandas as pd
import logging

DEFAULT_ARGS = {
    'start_date': days_ago(4),
    'owner': 'j-tsyrlin-3',
    'poke_interval': 600
}


def get_heading_by_week_day(**kwargs):
    today = datetime.today().weekday() + 1
    logging.info(f'Current week day: {today}')
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    pg_conn = pg_hook.get_conn()
    heading_df = pd.read_sql(f'SELECT heading FROM articles WHERE id = {today}', con=pg_conn)
    logging.info(f'Debug: {heading_df}')
    if heading_df.shape != (0, 0):
        query_res = heading_df['heading'].values[0]
        kwargs['task_instance'].xcom_push(key='j-tsyrlin-3_xcom_push', value=query_res)
    else:
        logging.info(f'Empty query received')


with DAG("j-tsyrlin-3_lesson5",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['j-tsyrlin-3']
         ) as dag:
    my_first_dummy = DummyOperator(task_id='dummy')

    bash_task = BashOperator(
        task_id='hello_bash',
        bash_command='echo Hello Airflow! Today is {{ a-gajdabura }}',
        dag=dag
    )

    python_task = PythonOperator(
        task_id='greenplum_heading',
        python_callable=get_heading_by_week_day,
        dag=dag
    )

    my_first_dummy >> [bash_task, python_task]


