"""
Airflow Hello world
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime


DEFAULT_ARGS = {
    'owner': 'm-chamov-4_4',
    'start_date': days_ago(2)
}

with DAG("m-chamov-4_4",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['m-chamov-4_4']
         ) as dag:


    def from_gp_to_xcom(**kwargs):
        execution_date = datetime.today().weekday()
        logging.info("execution_date:")
        logging.info(execution_date)

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute("SELECT heading FROM articles WHERE id = {0}".format(execution_date))
        query_res = cursor.fetchone()[0]

        kwargs['ti'].xcom_push(value=query_res, key='heading')
        conn.close()


    def from_xcom_to_log(**kwargs):
        log = kwargs['ti'].xcom_pull(task_ids='from_gp_to_xcom', key='heading')
        logging.info(log)


    t1 = PythonOperator(
        task_id='from_gp_to_xcom',
        provide_context=True,
        python_callable=from_gp_to_xcom,
        dag=dag)

    t2 = PythonOperator(
        task_id='from_xcom_to_log',
        provide_context=True,
        python_callable=from_xcom_to_log,
        dag=dag)


    t1 >> t2
