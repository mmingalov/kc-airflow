
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'g-epifanov',
    'poke_interval': 600
}

with DAG(dag_id="g-epifanov_hw_1",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         tags=['g-epifanov', 'hw-1']) as dag:

    dummy = DummyOperator(task_id='Dummy')

    echo_ds = BashOperator(
        task_id='actutal_date',
        bash_command='echo {{ ds_nodash }}',
        dag=dag
    )


    def hello_func():
        logging.info("Hello ")

    hello_task = PythonOperator(
        task_id='first_task',
        python_callable=hello_func,
        dag=dag
    )

    dummy >> [echo_ds, hello_task]


