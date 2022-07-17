"""
Это простейший даг.
Он проводит псевдо-жеребьевку, выбирает принимающую команду и команду-гостя
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import random

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'i-malahova-3',
    'poke_interval': 600
}

TEAMS_LIST = [
    'Зенит',
    'Кузбасс',
    'Динамо',
    'Локомотив',
    'Урал',
    'Искра'
]

with DAG("i-malahova-3_first_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['malahova']
         ) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')


    def choose_host():
        logging.info(random.choice(TEAMS_LIST))


    host = PythonOperator(
        task_id='host',
        python_callable=choose_host
    )


    def choose_guest():
        logging.info(random.choice(TEAMS_LIST))

    guest = PythonOperator(
        task_id='guest',
        python_callable=choose_guest
    )

start >> [host, guest] >> end
