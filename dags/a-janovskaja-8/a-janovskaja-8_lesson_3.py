'''
Gets weather information and passes it to log
'''

import requests
import re

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

WEATHER_URL = 'https://www.gismeteo.ru/weather-moscow-4368/'

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-janovskaja-8',
    'poke_interval': 600}

def log_weather_data():
    page_code = get_page_code(WEATHER_URL)
    if page_code:
        temperature = get_temp_value(page_code)
        temp_data = temperature if temperature else 'unavailable'
        logging.info(f'The temperature in Moscow is {temp_data}')


def get_page_code(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:78.0) Gecko/20100101 Firefox/78.0'}
    res = requests.get(url, headers=headers, timeout=10)
    if res.status_code == 200:
        return res.text
    else:
        logging.info('get_page_code failed')
        return None

def get_temp_value(text):
    pattern = re.compile('"temperatureAir":\[.*?\]')
    raw_temp = pattern.search(text)
    temp = raw_temp[0].split('[')[1][:-1]
    return temp

with DAG('a-janovskaja-8_lesson_3',
     schedule_interval='@hourly',
     default_args=DEFAULT_ARGS,
     max_active_runs=1,
     tags=['a-janovskaja-8']) as dag:

    start = DummyOperator(task_id='start')

    log_weather = PythonOperator(
        task_id='log_weather',
        python_callable=log_weather_data)

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}')

    start >> [echo_ds, log_weather]
