"""
Тестовый DAG
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import timedelta



DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-rjapisov',
    'poke_interval': 600
}


with DAG("a-rjapisov",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-rjapisov']) as dag:

    dummy = DummyOperator(task_id='dummy')

    bash = BashOperator(task_id='bash', bash_command='echo string')


    def pprint():
        print('hello, world')

    python = PythonOperator(task_id='python', python_callable=pprint)

    time = TimeDeltaSensor(task_id= 'time_delta', delta=timedelta(seconds=180))

    dummy >> bash >> time >> python


