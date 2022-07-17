"""
Данный DAG является простым будильником.
DAG запускается в будние дни в 4.30am по гринвичу или в 7.30am по местному времени.
DAG работает с 1 ноября 2021 года.
Через 10 секунд после запуска DAG'а в телеграм от бота приходит сообщение.
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    'owner': 'n.dubovik-2',
    'email': ['nikolaidubovik@gmail.com'],
    'start_date': datetime(2021, 11, 1),
    'poke_interval': 300,
    'retries': 3
}

with DAG(
        dag_id='n.dubovik_homework3',
        schedule_interval='30 7 * * 1-5',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['n.dubovik-2']
) as dag:

    start_time = BashOperator(
        task_id='print_start_time',
        bash_command='echo {{ a-gajdabura }}'
    )

    wait_10_sek = TimeDeltaSensor(
        task_id='wait_10_sek',
        delta=timedelta(seconds=10)
    )

    send_telegram = BashOperator(
        task_id='send_telegram',
        bash_command='python /var/lib/airflow/airflow.git/dags/n.dubovik-2/send_telegram.py'
    )

    finish_time = BashOperator(
        task_id='print_finish_time',
        bash_command='echo {{ a-gajdabura }}'
    )

start_time >> wait_10_sek >> send_telegram >> finish_time

dag.doc_md = __doc__
start_time.doc_md = """Пишет в логи время запуска DAG'а"""
wait_10_sek.doc_md = """Сенсор. Ждёт 10 секунд после запуска DAG'а"""
send_telegram.doc_md = """Выполняет запуск скрипта из файла send_telegram.py, который отправляет
                          собщение 'Проснись и пой, детка' в телеграм через телеграм бота"""
finish_time.doc_md = """Пишет в логи время окончания DAG'а"""


