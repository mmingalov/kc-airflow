"""
Test DAG
"""
from airflow import DAG
import logging
import pendulum

#from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': pendulum.today('UTC').add(days=-2),
    'owner': 'e-rachinskaja',
    'poke_interval': 600}

dag = DAG("e-rachinskaja-dag1",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['e-rachinskaja'])

dummy = DummyOperator(
    task_id="dummy",
    dag=dag)

def hello_world_func():
    logging.info("Hello World!")

hello_world = PythonOperator(
    task_id='hello_world',
    python_callable=hello_world_func,
    dag=dag)

dummy >> hello_world
