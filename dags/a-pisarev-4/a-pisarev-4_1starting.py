"""
 Starting DAG
 for triggering another DAG

 Here we have tasks for lesson 3

"""
import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# from airflow.operators.email import EmailOperator
from airflow.sensors.time_delta import TimeDeltaSensor

DEFAULT_ARGS = {
    'start_date': days_ago(0),
    'owner': 'a-pisarev-4',
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
    'poke_interval': 600,
    'trigger_rule': 'all_success'
}

with DAG(
    dag_id='a-pisarev-4_1starting_DAG',
    schedule_interval='@daily',     # '*/10 * * * *',       # ..every 10 minutes    '@hourly',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-pisarev-4']
) as dag:

    start_task = DummyOperator(
        task_id='start_task'
    )

    wait_2_minutes_task = TimeDeltaSensor(
        task_id='wait_2_minutes_task',
        delta=datetime.timedelta(seconds=2 * 60)
    )

    wait_1_minute_1_task = TimeDeltaSensor(
        task_id='wait_1_minute_1_task',
        delta=datetime.timedelta(seconds=1 * 60)
    )

    wait_1_minute_2_task = TimeDeltaSensor(
        task_id='wait_1_minute_2_task',
        delta=datetime.timedelta(seconds=2 * 60)
    )

    def func_log_info():
        logging.info("we've got message from Python Operator")

    task_with_PythonOperator = PythonOperator(
        task_id="task_with_PythonOperator",
        python_callable=func_log_info
    )

    task_with_BashOperator = BashOperator(
        task_id="task_with_BashOperator",
        bash_command="echo {{ ts }}"
    )

    dummy_task = DummyOperator(
        task_id="dummy_task"
    )

    # task_with_EmailOperator = EmailOperator(
    #     task_id="task_with_EmailOperator",
    #     to="andrii.pysarev@gmail.com",
    #     subject="Airflow: task_with_EmailOperator",
    #     html_content="Hello! =) Now is {{ ts }}"
    # )

    end_task = DummyOperator(
        task_id='end_task',
        trigger_rule='all_success'
    )

    start_task >> [wait_2_minutes_task, wait_1_minute_1_task] >> task_with_BashOperator >> [dummy_task, task_with_PythonOperator] >> wait_1_minute_2_task >> end_task