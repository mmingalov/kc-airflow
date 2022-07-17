import logging
from datetime import datetime

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator


def collect_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    query = f"SELECT heading FROM articles WHERE id = {int(kwargs['weekday'])}"
    logging.info(f"Execute query: {query}")
    cursor.execute(query)
    result = cursor.fetchall()
    logging.info(result)
    kwargs['ti'].xcom_push(value=result, key='article_headings')


default_args = {
    'owner': 'j-dylko-3',
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2021, 12, 1),
    'end_date': datetime(2021, 12, 31),
    'retries': 2,
}

with DAG(
        dag_id='j-dylko-3-dag-one',
        default_args=default_args,
        schedule_interval='@daily',
        description='Даг по домашнему заданию №1',
        max_active_runs=1,
        concurrency=2
) as main_dag:

    echo_day_of_week = BashOperator(
        task_id='bash_operator',
        bash_command="echo Today is {{ execution_date.format('dddd') }}",
    )

    skip_sunday = ShortCircuitOperator(
        task_id='skip_sunday',
        python_callable=lambda *args: int(args[0]) != 0,
        op_args=["{{ execution_date.format('d') }}"]
    )

    fetch_data_from_gp = PythonOperator(
        task_id='fetch_data_from_gp',
        python_callable=collect_data,
        op_kwargs={
            'weekday': "{{ execution_date.format('d') }}"
        },
        provide_context=True
    )

    echo_day_of_week >> skip_sunday >> fetch_data_from_gp
