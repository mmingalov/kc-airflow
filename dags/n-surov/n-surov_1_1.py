"""
DAG для ДЗ 1
строка 2
строка 3
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(5),
    'owner': 'n-surov',
    'poke_interval': 120
}

with DAG("n-surov_1_2",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=10,
          tags=['n-surov']
          ) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ a-gajdabura }}',
        dag=dag
    )

    def hello_world_func():
        logging.info("Hello AirFlow")


    hello_world = PythonOperator(
        task_id='first_task',
        python_callable=hello_world_func,
        dag=dag
    )


    dummy >> [echo_ds, hello_world]

dag.doc_md = __doc__

hello_world.doc_md = """Привет земляне"""
echo_ds.doc_md = """Тест дока"""