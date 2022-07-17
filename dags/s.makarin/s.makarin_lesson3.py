"""
Тестовый даг из 3 урока
Он состоит из сенсора (ждёт 2pm),
баш-оператора (выводит execution_date),
двух питон-операторов (выводят по строке в логи)
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(5),
    'owner': 's.makarin',
    'poke_interval': 600
}

with DAG("s.makarin_l3_dag",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s.makarin']
) as dag:

    wait_until_2pm = TimeDeltaSensor(
        task_id='wait_until_2pm',  # уникальный идентификатор таски внутри DAG
        delta=timedelta(seconds=14 * 60 * 60)  # время, которое мы ждем от запуска DAG
    )

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def first_func():
        logging.info("Все хорошо")

    first_task = PythonOperator(
        task_id='first_task',
        python_callable=first_func,
        dag=dag
    )

    def second_func():
        logging.info("Все очень хорошо")


    second_task = PythonOperator(
        task_id='second_task',
        python_callable=second_func,
        dag=dag
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule='one_success',
        dag=dag
    )

    wait_until_2pm >> echo_ds >> [first_task, second_task] >> end

    dag.doc_md = __doc__
    wait_until_2pm.doc_md = """Сенсор. Ждёт наступление 2pm по Гринвичу"""
    echo_ds.doc_md = """Пишет в лог execution_date"""
    first_task.doc_md = """Пишет в лог 'Все хорошо'"""
    second_task.doc_md = """Пишет в лог 'Все очень хорошо'"""
    end.doc_md = """Ничего не делает. trigger_rule='one_success'"""