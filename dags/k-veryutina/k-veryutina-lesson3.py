"""
Lesson 3
"""
from airflow import DAG
from datetime import timedelta, datetime
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.time_delta_sensor import TimeDeltaSensor

import numpy as np

DEFAULT_ARGS = {
    'start_date': datetime(2022, 6, 1),
    'owner': 'krisssver',
    'email': ['kristina_veryutina@mail.ru'],
    'email_on_failure': True,
    'retries': 4,
    'sla':timedelta(hours=1),
    'poke_interval': 600
}

with DAG("k-veryutina-lesson3",
         schedule_interval='0 10 * * *',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['krisssver','lesson3']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    echo_ts = BashOperator(
        task_id='echo_ts',
        bash_command='echo {{ ts }}',
        dag=dag
    )

    def generate_random_list():
        logging.info('Random List:' + str(np.random.randint(-10, 10, 10)))


    generate_random_list = PythonOperator(
        task_id='generate_random_list',
        python_callable=generate_random_list,
        dag=dag
    )

    wait_15sec = TimeDeltaSensor(
        task_id='wait_15sec',
        delta=timedelta(seconds=15),
    )

    def task_ok():
        logging.info("Finished OK")

    task_end = PythonOperator(
        task_id='task_ok',
        python_callable=task_ok,
        dag=dag
    )

    dummy >> [echo_ds, echo_ts] >> wait_15sec >> generate_random_list >> task_end