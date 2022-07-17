"""
Тестовый даг - 001
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-mogilnyj-7',
    'poke_interval': 600
}

with DAG("a-mogilnyj-7__001",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-mogilnyj-7__001']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def dag_infolog_func():
        logging.info("DAG Infolog")

    dag_infolog = PythonOperator(
        task_id='DAG_Infolog',
        python_callable=dag_infolog_func,
        dag=dag
        )
    dummy >> [echo_ds, dag_infolog]