"""
Выбираем счастливое число в этом месяце :)
Счастливое число lucky_day выбирается случайным образом
Если lucky_day >= текущей даты current_day, то будет выведено сообщение таска this_month
Иначе будет выведено сообщение таска next_month
"""

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

import calendar
from datetime import date
import random

def _choose_lucky_day():
    print('Choosing lucky day...')
    current_year = date.today().year
    current_month = date.today().month
    current_day = date.today().day
    lucky_day = random.choice(range(1, calendar.monthrange(current_year, current_month)[1] + 1))
    print(f'This month your lucky day is {lucky_day} {calendar.month_abbr[current_month]} {current_year}')

    if lucky_day >= current_day:
        return 'this_month'
    return 'next_month'


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'poke_interval': 600
}

with DAG("m-nemirovskaja-3_part_1", schedule_interval='@daily',
         default_args=DEFAULT_ARGS, max_active_runs=1, tags=['m-nemirovskaja-3']) as dag:
    dummy = DummyOperator(task_id='dummy')

    choose_lucky_day = BranchPythonOperator(
        task_id='choose_lucky_day',
        python_callable=_choose_lucky_day,
    )

    this_month = BashOperator(
        task_id='this_month',
        bash_command='echo "Take your chance! :)"'
    )

    next_month = BashOperator(
        task_id='next_month',
        bash_command='echo "Be prepared for the future! :)"'
    )

    dummy >> choose_lucky_day >> [this_month, next_month]