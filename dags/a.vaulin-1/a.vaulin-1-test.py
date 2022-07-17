import datetime
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(5),
    'owner': 'Karpov',
    'poke_interval': 600,
    'catchup': False,
}


with DAG("a.vaulin-1-test",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['karpov', 'vaulin']
         ) as dag:

    start = DummyOperator(task_id='start')
    finish = DummyOperator(task_id='finish')

    def get_heading_by_id(**kwargs):

        ts = datetime.datetime.strptime(kwargs.get('a-gajdabura'), '%Y-%m-%d')
        weekday_num = ts.weekday() + 1

        pg_hook = PostgresHook('conn_greenplum')
        records = pg_hook.get_records(f'SELECT heading FROM articles WHERE id = {weekday_num}')

        return records[0]

    get_heading_by_id = PythonOperator(
        task_id='get_heading_by_id',
        python_callable=get_heading_by_id,
        provide_context=True
    )

    start >> get_heading_by_id >> finish
