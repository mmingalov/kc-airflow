"""
The first DAG I've written.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'a-kolesnik-3',
    'poke_interval': 600
}

with DAG("a-kolesnik-3_first_dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-kolesnik-3']
          ) as dag:

    begin = DummyOperator(task_id='begin')

    write_datetime = BashOperator(
        task_id='write_datetime',
        bash_command='echo {{ ts }}',
        dag=dag
    )

    def hello_world():
        print('Hello, World! Nice to see you!')

    print_hello = PythonOperator(
        task_id='print_hello',
        python_callable=hello_world,
        dag=dag
    )

    end = DummyOperator(task_id='end')

    begin >> [write_datetime, print_hello] >> end
