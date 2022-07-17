"""
Забираем данные с CBR и кладем в Greenplum
"""
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

from datetime import datetime

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'd-grigorev',
    'poke_interval': 600
}

with DAG("d-grigorev-art-dag",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['d-grigorev'],
         catchup=True
         ) as dag:
    start = DummyOperator(task_id='start', dag=dag)


    def greenplum_hook(**kwargs):
        ts = kwargs['execution_date']
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        logging.info(f"День недели - {ts.weekday() + 1}")
        cursor.execute(f"select heading from articles where id={ts.weekday() + 1}")
        query_res = cursor.fetchall()
        kwargs['ti'].xcom_push(key='article', value=query_res)


    get_data = PythonOperator(
        task_id='get_data',
        python_callable=greenplum_hook,
        provide_context=True,
        dag=dag
    )

    end = DummyOperator(task_id='end', dag=dag)

    start >> get_data >> end
