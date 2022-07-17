"""
Простой даг для пробы
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'e-nikitin-7',
    'poke_interval': 600
}

with DAG("e-nikitin-7-hw1",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['e-nikitin-7']
          ) as dag:

    dummy = DummyOperator(task_id='dummy')

    echo_ds = BashOperator(
        task_id='bash_echo',
        bash_command='echo "hello from echo"',
        dag=dag
    )

    def log_ds_func():
        logging.info("hello from python")

    log_ds = PythonOperator(
        task_id='python_log',
        python_callable=log_ds_func,
        dag=dag
    )

    dummy >> [echo_ds, log_ds]
