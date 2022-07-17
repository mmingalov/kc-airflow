"""
Hello world dag
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'a.petuhov_1',
    'poke_interval': 600
}

dag = DAG("a.petuhov_1_simple_dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a.petuhov_1']
          )

wait_until_6am = TimeDeltaSensor(
    task_id='wait_until_6am',
    delta=timedelta(seconds=6*60),
    dag=dag
)

bash_echo_ds = BashOperator(
    task_id='bash_echo_ds',
    bash_command='echo {{ a-gajdabura }}',
    dag=dag
)


def hello_func():
    logging.info("PythonOperator: Hello world")


hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=hello_func,
    dag=dag
)


wait_until_6am >> bash_echo_ds >> hello_task

dag.doc_md = __doc__

wait_until_6am.doc_md = """Сенсор. Ждёт наступления 6am по Гринвичу"""
bash_echo_ds.doc_md = """Пишет в лог execution_date"""
hello_task.doc_md = """Пишет в лог 'Hello world'"""

