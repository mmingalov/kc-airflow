import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(5),
    'owner': 'a-vdovenko',
    'poke_interval': 600
}

with DAG("a-vdovenko",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-vdovenko_lesson_5']
         ) as dag:

    echo = BashOperator(
        task_id='echo',
        bash_command='echo "Now: {{ ts }}"',
        dag=dag
    )

    dummy = DummyOperator(task_id='dummy')

    def gp_push_to_xcom(**kwargs):
        
        data_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = data_hook.get_conn()
        cursor = conn.cursor()

        
        day_of_week = datetime.datetime.today().isoweekday()

        
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')
        heading_string = cursor.fetchall()

        
        logging.info('--------')
        logging.info(f'Weekday: {day_of_week}')
        logging.info(f'Return value: {heading_string}')
        logging.info('--------')

        
        kwargs['ti'].xcom_push(value=heading_string, key='heading')

    xcom_push = PythonOperator(
        task_id='xcom_push',
        python_callable=gp_push_to_xcom,
        provide_context=True
    )

    echo >> dummy >> xcom_push
