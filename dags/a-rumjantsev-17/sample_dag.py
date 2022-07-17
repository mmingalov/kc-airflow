"""
Простой даг для пробы
после просмотра первой практики
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-rumjantsev-17',
    'poke_interval': 600
}

with DAG("ae_new_dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-rumjantsev-17']
          ) as dag:

    dummy = DummyOperator(task_id='dummy')

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ a-gajdabura }}',
        dag=dag
    )

    def hello_ae_func():
        logging.info("Hello world/nHello, ae")

    hello_func = PythonOperator(
        task_id='hello_world',
        python_callable=hello_ae_func,
        dag=dag
    )

    dummy >> [echo_ds, hello_func]


