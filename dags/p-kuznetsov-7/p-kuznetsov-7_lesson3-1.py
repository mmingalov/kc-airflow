from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'p-kuznetsov-7',
    'poke_interval': 600
}

with DAG("p_kuznetsov_7_lesson3_1_dag",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['p-kuznetsov-7']
) as dag:

    dummy = DummyOperator(task_id="p_kuznetsov_7_dummy")

    echo_p_kuznetsov_7 = BashOperator(
        task_id='whoami',
        bash_command='whoami',
        dag=dag
    )

    def hello_world_func():
        logging.info("Hello World!")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )

    def heading_from_articles_func(**kwargs):
        execution_date = datetime.today()
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        weekday = execution_date.weekday() + 1
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')
        query_res = cursor.fetchall()
        kwargs['ti'].xcom_push(value=query_res, key='heading')
        logging.info(query_res)

    heading_from_articles = PythonOperator(
        task_id='heading_from_articles',
        python_callable=heading_from_articles_func,
        provide_context=True,
        dag=dag
    )


    dummy >> [echo_p_kuznetsov_7, hello_world, heading_from_articles]