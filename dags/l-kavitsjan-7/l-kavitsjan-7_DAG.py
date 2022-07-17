'''
Даг
Кавицян Л М

В определенное время суток:
- вызывает расчет факториала текущего дня
'''

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'l-kavitsjan-7',
    'poke_interval': 600,
    'email': ['myworkpython@mail.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
}


def fact(number):
    '''
    Функция для вычисления  факориала целого числа
    '''
    if number > 1:
        return number * fact(number - 1)
    else:
        return 1


def calc_factorial(**kwargs):
    '''
    Процедура для вывода в логи результата расчета факториала
    '''
    logging.info('-' * 100)
    logging.info('Pass Number: ' + str(kwargs['number']))
    logging.info('Result: ' + str(fact(kwargs['number'])))
    logging.info('Status: ' + str(kwargs['status']))
    logging.info('-' * 100)


def on_failure_calc_factorial():
    '''
    В случае, если вычисление факториала вывалилось с ошибкой
    исполняется данная функция в другом таске
    '''
    logging.info('-' * 100)
    logging.info('Calc Factorial End With Error, check DAG script!')
    logging.info('-' * 100)


with DAG(
    dag_id='l-kavitsijan-7-dag',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['l-kavitsjan-7'],
) as dag:

    start_dummy = DummyOperator(
        task_id='startDag',
    )

    bash_task_echo = BashOperator(
        task_id='curr_datetime',
        bash_command='echo {{ macros.datetime.now() }}',
    )

    python_task_log_print1 = PythonOperator(
        task_id='print_number_factorial1',
        python_callable=calc_factorial,
        op_kwargs={'number': int(datetime.now().day), 'status': 'OK'},
    )

    python_task_log_print2 = PythonOperator(
        task_id='print_number_factorial2',
        python_callable=on_failure_calc_factorial,
        trigger_rule='one_failed',
    )

    start_dummy >> bash_task_echo >> python_task_log_print1 >> python_task_log_print2




