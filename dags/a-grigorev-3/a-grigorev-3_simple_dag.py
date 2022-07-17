"""
Hello world dag(мой первый даг)
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-grigorev-3',
    'poke_interval': 600
}

dag = DAG("a-grigorev-3_simple_dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-grigorev-3']
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


right_task = PythonOperator(
    task_id='right_task',
    python_callable=hello_func,
    dag=dag
)

def another_hello_func():
    logging.info("PythonOperator: Hello another world")


left_task = PythonOperator(
    task_id='left_task',
    python_callable=another_hello_func,
    dag=dag
)

wait_until_6am >> bash_echo_ds >> [left_task, right_task]

dag.doc_md = __doc__

wait_until_6am.doc_md = """Сенсор. Ждёт наступления 6am по Гринвичу"""
bash_echo_ds.doc_md = """Пишет в лог execution_date"""
right_task.doc_md = """Пишет в лог 'Hello world'"""
left_task.doc_md = """Пишет в лог 'Hello another world'"""
