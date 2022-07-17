"""
Простой тренировочный даг.
Запускается ежедневно в 4 утра, начиная с 2021-11-07.
Cостоит из сенсора (ожидает 4am),
баш-оператора (выводит дату выполнения),
двух питон-операторов (выводят по строке в логи),
оператора ветвления (выбирает случайным образом одну из задач для запуска.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from datetime import timedelta
import logging

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2021, 11, 7),
    'owner': 'NDavydova',
    'poke_interval': 600,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['nitgrd2@ya.ru', ]
}

with DAG(
        "ndavydova_simple_dag",
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['karpov_courses'],
) as dag:

    wait_4am = TimeDeltaSensor(
        task_id='wait_4am',
        delta=timedelta(seconds=4 * 60 * 60)
    )
    print_ds = BashOperator(
        task_id='print_ds',
        bash_command='echo {{ a-gajdabura }}'
    )

    def first_func():
        logging.info("First log!")

    first_task = PythonOperator(
        task_id='first_task',
        python_callable=first_func
    )

    def second_func():
        logging.info("Second log!")


    second_task = PythonOperator(
        task_id='second_task',
        python_callable=second_func
    )

    def select_random_func():
        return random.choice(['task_1', 'task_2', 'task_3'])

    select_random = BranchPythonOperator(
        task_id='select_random_task',
        python_callable=select_random_func
    )
    task_1 = DummyOperator(task_id='task_1')
    task_2 = DummyOperator(task_id='task_2')
    task_3 = DummyOperator(task_id='task_3')

wait_4am >> print_ds >> first_task >> select_random >>  second_task

dag.doc_md = __doc__
wait_4am.doc_md = """Сенсор. Ждёт наступления 6am по Гринвичу"""
print_ds.doc_md = """Пишет дату выполнения"""
first_task.doc_md = """Пишет в лог 'First log!'"""
second_task.doc_md = """Пишет в лог 'Second log!'"""
select_random.doc_md = """Случайным образом выбирает одну из 3 тасков для запуска"""
