
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'e-abroskina-7',
    'start_date': days_ago(0),
    'depends_on_past': False
}
with DAG(
    dag_id='e-abroskina-7_first_dag',
    schedule_interval='@daily',
    default_args=default_args,
    max_active_runs=1,
    tags=['e-abroskina-7']
) as dag:
    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )


    def hello_world_func():
        logging.info("Hello World!")


    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )

    dummy >> [echo_ds, hello_world]