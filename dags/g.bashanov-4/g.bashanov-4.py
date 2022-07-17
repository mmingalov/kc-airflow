from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'g.bashanov-4',
    'poke_interval': 600
}

with DAG(
    dag_id="g.bashanov-4",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['g.bashanov-4']
) as dag:

    def hello_world_func():
        logging.info("Hello World!")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )


hello_world