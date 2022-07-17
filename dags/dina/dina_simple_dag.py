"""
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

DEFAULT_ARGS = {
    'start_date': days_ago(12),
    'owner': 'Karpov',
    'poke_interval': 600
}

dag = DAG("dina_simple_dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['karpov']
          )

wait_until_6am = TimeDeltaSensor(
    task_id='wait_until_6am',
    delta=timedelta(seconds=6*60*60),
    dag=dag
)

echo_ds = BashOperator(
    task_id='echo_ds',
    bash_command='echo {{ a-gajdabura }}',
    dag=dag
)


def first_func():
    logging.info("First log")


first_task = PythonOperator(
    task_id='first_task',
    python_callable=first_func,
    dag=dag
)


def second_func():
    logging.info("Second log")


second_task = PythonOperator(
    task_id='second_task',
    python_callable=second_func,
    dag=dag
)

wait_until_6am >> echo_ds >> [first_task, second_task]

dag.doc_md = __doc__

wait_until_6am.doc_md = """Сенсор. Ждёт наступления 6am по Гринвичу"""
echo_ds.doc_md = """Пишет в лог execution_date"""
first_task.doc_md = """Пишет в лог 'First log'"""
second_task.doc_md = """Пишет в лог 'Second log'"""
