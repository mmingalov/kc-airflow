from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import date
import logging


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'ds',
    'poke_interval': 600
}

with DAG("m-shakirov_dags",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['m-shakirov']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def start_career_func():
        date_today = date.today().strftime("%Y-%m-%d")
        logging.info(f'Starting Data Engineer career in {date_today}')

    start_career = PythonOperator(
        task_id='start_career',
        python_callable=start_career_func,
        dag=dag
    )

    dummy >> [echo_ds, start_career]