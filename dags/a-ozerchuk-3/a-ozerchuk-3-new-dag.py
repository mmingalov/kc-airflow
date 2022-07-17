""""
Это простейший даг.
Он состоит из сенсора (ждёт 6am),
баш-оператора (выводит execution_date),
двух питон-операторов (выводят по строке в логи)
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-ozerchuk-3',
    'poke_interval': 600
}

with DAG("a_ozerchuk_simple_dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-ozerchuk-3']
          ) as dag:

    dummy = DummyOperator(task_id = 'dummy'     )


    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ a-gajdabura }}',
        dag=dag
    )


    def log_ds_func():
        logging.info("{{ a-gajdabura }}")


    log_ds = PythonOperator(
        task_id='first_task',
        python_callable=log_ds_func,
        dag=dag
    )

dummy >> [echo_ds, log_ds]
