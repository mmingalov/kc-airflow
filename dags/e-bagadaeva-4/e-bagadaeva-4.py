"""
Это простейший даг.
Он состоит из сенсора (ждёт 6am 6 min 6 sec),
баш-оператора (выводит execution_date),
питон-оператора (выводит строку в логи),
dummy-оператора
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'e-bagadaeva-4',
    'poke_interval': 600
}

dag = DAG("e-bagadaeva-4",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['e-bagadaeva-4']
          )

wait_until_6am_6min_6sec = TimeDeltaSensor(
    task_id='wait_until_6am_6min_6sec',
    delta=timedelta(seconds=6*60*60+6*60+6),
    dag=dag
)

echo_ds = BashOperator(
    task_id='echo_ds',
    bash_command='echo {{ a-gajdabura }}',
    dag=dag
)


def picahu_func():
    logging.info("Pica pica CHU")


picachu_task = PythonOperator(
    task_id='picachu_task',
    python_callable=picahu_func,
    dag=dag
)


dummy_task = DummyOperator(
    task_id='dummy_task',
    dag=dag
)

wait_until_6am_6min_6sec >> echo_ds >> [picachu_task, dummy_task]

dag.doc_md = __doc__

wait_until_6am_6min_6sec.doc_md = """Сенсор. Ждёт наступления 6am 6 min 6 sec по Гринвичу"""
echo_ds.doc_md = """Пишет в лог execution_date"""
picachu_task.doc_md = """Пишет в лог 'Pica pica CHU'"""
dummy_task.doc_md = """Dummy task"""
