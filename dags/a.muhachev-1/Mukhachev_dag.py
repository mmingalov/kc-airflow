from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'a.muhachev',
    'depends_on_past': False,
    'start_date': datetime(2021, 10, 24),
    'retries': 0
}
with DAG("Test_Mukhachev_Dag",
         schedule_interval='@daily',
         default_args=default_args,
         max_active_runs=1,
         tags=['a.mukhachev']
         ) as dag:

    start = DummyOperator(task_id='start_m')
    end = DummyOperator(task_id='end_m')

    def print_func():
        print('My name is Artem Mukhachev. Not Muhachev Artem')

    print_task = PythonOperator(
        task_id='print_task',
        python_callable=print_func
    )


    start >> print_task >> end