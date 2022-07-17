"""
Простой dag
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime
import calendar


def is_weekend():
    day = datetime.now().weekday()
    if day < 5:
        return "Иди работай!"
    else:
        return "Сегодня выходной"


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'd-grigorev',
    'poke_interval': 600
}

with DAG("d-grigorev-simple-dag",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['d-grigorev'],
    user_defined_macros={'my_custom_macro': is_weekend()}
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_is_weekend',
        bash_command='echo {{ my_custom_macro }}',
        dag=dag
    )

    def today_date():
        logging.info(f"Сегодня {datetime.now()}")

    today_date = PythonOperator(
        task_id='today_date',
        python_callable=today_date,
        dag=dag
    )

    def check_leap():
        if calendar.isleap(datetime.now().year):
            print("Год високосный")
        else:
            print("Год невисокосный")

    leap = PythonOperator(
        task_id='leap_year',
        python_callable=check_leap,
        dag=dag
    )


    dummy >> [echo_ds, today_date, leap]