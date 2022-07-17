"""
Задание к уроку №3 "Сложные пайплайны, часть 1"
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v-zotov',
    'poke_interval': 600
}

with DAG("v-zotov",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['v-zotov']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )


    def hello_world_func():
        logging.info("Hello World! This is Lesson 3")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )


    def lovely_func():
        logging.info("It works. What a lovely day!")

    lovely_work = PythonOperator(
        task_id='lovely_work',
        python_callable=lovely_func,
        dag=dag
    )

    sleep_minute = BashOperator(
        task_id='sleep_minute',
        bash_command='sleep 60',
        dag=dag
    )


    def bye_bye_func():
        logging.info("See you soon!")

    bye = PythonOperator(
        task_id='bye',
        python_callable=bye_bye_func,
        dag=dag
    )

    dummy >> [echo_ds, hello_world] >> lovely_work >> sleep_minute >> bye
