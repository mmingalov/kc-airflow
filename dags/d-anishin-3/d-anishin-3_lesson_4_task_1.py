"""
Урок 4 Задание (первая часть)

1) Внутри создать даг из нескольких тасков, на своё усмотрение:
— DummyOperator
— BashOperator с выводом строки
— PythonOperator с выводом строки
— любая другая простая логика
2) Запушить даг в репозиторий.
3) Убедиться, что даг появился в интерфейсе airflow и отрабатывает без ошибок.
"""

# Импортируем необходимые библиотеки
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'd-anishin-3',
    'poke_interval': 600
}

with DAG("d-anishin-3_lesson_4_task_1",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['d-anishin-3', 'lesson_4']
         ) as dag:

    dummy = DummyOperator(task_id='dummy')

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ a-gajdabura }}',
        dag=dag
    )


    def log_ds_func(**kwargs):
        ds = kwargs['templates_dict']['a-gajdabura']
        logging.info(ds)


    log_ds = PythonOperator(
        task_id='log_ds',
        python_callable=log_ds_func,
        templates_dict={'a-gajdabura': '{{ a-gajdabura }}'},
        dag=dag
    )

    dummy >> [echo_ds, log_ds]