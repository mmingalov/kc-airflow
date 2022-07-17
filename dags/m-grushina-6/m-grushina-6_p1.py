"""
Даг из урока Сложные пайплайны Часть 1
Запускает две ветки задач, первая выводит дату исполнения,
вторая выполняется только в рабочий день
"""


from airflow import DAG
from datetime import datetime

from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm-grushina',
    'poke_interval': 600
}

with DAG("m-grushina_p1",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['m-grushina']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
        )

    def workdays_func():
        logging.info("СЕГОДНЯ РАБОЧИЙ ДЕНЬ")


    workdays_str = PythonOperator(
        task_id='workdays_str',
        python_callable=workdays_func,
        trigger_rule='none_failed',
        dag=dag
    )

    def is_workdays_func(execution_dt):
        logging.info(execution_dt)
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day not in [5, 6]

    workdays_only = ShortCircuitOperator(
        task_id='workdays_only',
        python_callable=is_workdays_func,
        op_kwargs= {'execution_dt' : '{{ ds }}'}
    )
    end = BashOperator(
        task_id='end',
        bash_command='echo "THE END"',
        trigger_rule='one_success'
    )

    dummy >> echo_ds >> end
    dummy >> workdays_only >> workdays_str >> end

