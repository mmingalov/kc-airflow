from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator, BranchPythonOperator
from airflow.sensors.weekday import DayOfWeekSensor

from datetime import datetime

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm-averbah-4',
    'poke_interval': 600
}

with DAG("m_averbah_4_HW",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['mahw']
) as dag:

    start = DummyOperator(task_id="start")

    """" функция определяет четный или нечетный день"""
    def even_odd_func(execution_date):
        exec_day=datetime.strptime(execution_date, '%Y-%m-%d').day
        if exec_day%2==0:
            return ['echo_even']
        return['echo_odd']

    """если день четный вызывается функция, печатающая 'even', если нечетный 'odd'"""
    even_odd=BranchPythonOperator(
        task_id='even_odd',
        python_callable=even_odd_func,
        op_kwargs={'execution_date': '{{a-gajdabura}}'}
    )

    echo_even=BashOperator(
        task_id='echo_even',
        bash_command='echo even'
    )

    echo_odd=BashOperator(
        task_id='echo_odd',
        bash_command='echo even'
    )

    """Если сегодня пятница, выводим на печать Thanks God it's Friday"""
    TGIF_sensor=DayOfWeekSensor(
        task_id='is_it_friday',
        week_day={'Friday'}
    )

    def TGIF_func():
        logging.info("Thanks God it's Friday")

    TGIF=PythonOperator(
        task_id='TGIF',
        python_callable=TGIF_func
    )

    start>>[even_odd,TGIF_sensor]
    even_odd>>[echo_even,echo_odd]
    TGIF_sensor>>TGIF