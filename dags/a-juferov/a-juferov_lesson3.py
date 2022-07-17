"""
Получает курс валют, выводит в лог
"""
import requests
import json
import logging
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-juferov',
    'poke_interval': 600
}


def print_current_exchange_rate(date):
    res = requests.get(f'https://api.exchangerate.host/timeseries?start_date={date}&end_date={date}&base=USD&symbols=RUB&format=json').content.decode('utf-8')
    res = json.loads(res)
    logging.info(f"Курс USD на {date}: {res['rates'][date]['RUB']} RUB")


with DAG("a-juferov_lesson3",
     schedule_interval='@daily',
     default_args=DEFAULT_ARGS,
     max_active_runs=1,
     tags=['a-juferov']) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}'
    )

    current_exchange_rate = PythonOperator(
        task_id='current_exchange_rate',
        python_callable=print_current_exchange_rate,
        op_args=['{{ ds }}']
    )

    dummy >> [echo_ds, current_exchange_rate]
