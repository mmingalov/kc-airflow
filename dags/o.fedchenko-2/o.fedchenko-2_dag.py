"""
Это первый даг.
Он состоит из сенсора -  ждет 12 am,
баш-оператора  - выводит дату исполнения,
двух питон-операторов - выводят по строке в логи
"""

from airflow import DAG
from datetime import datetime
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(10),
    'owner': 'o.fedchenko-2',
    'poke_interval': 600
}

dag = DAG("o.fedchenko-2_dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['karpov']
          )

wait_until_12am = TimeDeltaSensor(
    task_id='wait_until_12am',
    delta=timedelta(seconds=12*60*60),
    dag=dag
)

echo_ds = BashOperator(
    task_id='echo_ds',
    bash_command='echo {{ a-gajdabura }}',
    dag=dag
)


def first_string_func():
    logging.info("Buenas tardes, Raymondo!")


first_task = PythonOperator(
    task_id='first_task',
    python_callable=first_string_func,
    dag=dag
)


def second_string_func():
    logging.info("Buenas tardes,Fletcher-mondo!")


second_task = PythonOperator(
    task_id='second_task',
    python_callable=second_string_func,
    dag=dag
)

wait_until_12am >> echo_ds >> [first_task, second_task]