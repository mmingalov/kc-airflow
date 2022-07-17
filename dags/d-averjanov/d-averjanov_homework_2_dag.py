from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import get_current_context

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1, 9, 30),
    'end_date': datetime(2022, 3, 14, 9, 30),
    'owner': 'd-averjanov',
    'poke_interval': 600
}

with DAG("d-averjanov_homework_2",
        schedule_interval='30 9 * * 1-6',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['d-averjanov']
        ) as dag:

    dummy_start = DummyOperator(task_id='start')

    def extract_data_func(**kwargs):
        context = get_current_context()
        ds = context["ds"]
        weekday = datetime.strptime(ds, '%Y-%m-%d').weekday()+1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(weekday))
        query_res = cursor.fetchall()
        logging.info(f'Value: {query_res}, Weekday: {weekday}, Date: {ds}')
        kwargs['ti'].xcom_push(value=query_res, key='article')
        

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data_func,
        provide_context=True,
        dag=dag
    )

    dummy_end = DummyOperator(task_id='end')

    dummy_start >> extract_data >> dummy_end
