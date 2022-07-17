"""
ОПИСАНИЕ
В данном ДАГе представлены оба задания
Задание 1 (простые операторы) - заканчивается на таске dummy
Задание 2 (взять данные из БД и сохранить в XCom) - таск read_from_greenplum
choose_lucky_day >> [this_month, next_month] >> dummy >> read_from_greenplum

Задание 1. Простые операторы
Выбираем счастливое число в этом месяце :)
Счастливое число lucky_day выбирается случайным образом
Если lucky_day >= текущей даты current_day, то будет выведено сообщение таска this_month
Иначе будет выведено сообщение таска next_month

Задание 2. Забрать данные из articles и сохранить соответствующую статью в XCom
Задание представлено таском read_from_greenplum
"""

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook

import calendar
from datetime import date, datetime
import random

# Задание 1. Написать функцию, возвращающую результат
def choose_lucky_day():
    """
    Функция возвращает случайное число месяца.
    Если lucky_day >= текущей даты current_day, то переходим к таску this_month; иначе - next_month.
    """
    print('Choosing lucky day...')
    current_year = date.today().year
    current_month = date.today().month
    current_day = date.today().day
    lucky_day = random.choice(range(1, calendar.monthrange(current_year, current_month)[1] + 1))
    print(f'This month your lucky day is {lucky_day} {calendar.month_abbr[current_month]} {current_year}')

    if lucky_day >= current_day:
        return 'this_month'
    return 'next_month'


# Задание 2. Написать функцию, сохраняющую результат из БД в XCom
def read_from_greenplum_func(ti, **context):
    """
    Функция берет из GreenPlum номер статьи, соответствующий дню недели.
    Далее заголовок этой статьи сохраняется в XCom.
    """
    # учтено, что 0 - это Monday, 6 - это Sunday
    execution_date = context['execution_date']
    article_num = execution_date.weekday() + 1
    if article_num == 7:
        ti.xcom_push(key='karpov_result_alert', value='The returned value is "Sunday" - check "day of week"!')
    else:
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("article_cursor")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {article_num}')
        #query_res = cursor.fetchall()
        one_string = cursor.fetchone()[0]
        ti.xcom_push(key=f'article_result_{execution_date}', value=one_string) # для самопроверки отражаем в названии execution_date


DEFAULT_ARGS = {
    #'start_date': datetime(2021, 12, 6), # для самопроверки сделаем побольше запусков
    'start_date': days_ago(6), # для самопроверки сделаем побольше запусков
    'poke_interval': 600
}

with DAG("m-nemirovskaja-3_part_2", schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS, tags=['m-nemirovskaja-3'], max_active_runs=1, catchup=True) as dag:

    choose_lucky_day = BranchPythonOperator(
        task_id='choose_lucky_day',
        python_callable=choose_lucky_day
    )

    this_month = BashOperator(
        task_id='this_month',
        bash_command='echo "Take your chance! :)"',
        do_xcom_push=False
    )

    next_month = BashOperator(
        task_id='next_month',
        bash_command='echo "Be prepared for the future! :)"',
        do_xcom_push=False
    )

    dummy = DummyOperator(
        task_id='dummy',
        trigger_rule='one_success'
    )

    read_from_greenplum = PythonOperator(
        task_id='read_from_greenplum',
        python_callable=read_from_greenplum_func,
        provide_context=True
    )

    choose_lucky_day >> [this_month, next_month] >> dummy >> read_from_greenplum