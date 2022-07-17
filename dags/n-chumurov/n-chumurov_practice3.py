"""
Тестовый даг

"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from random import randint

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'n-chumurov'
}

def heads_or_tails(rand_int):
    heads_or_tails_dict = {
        1:'орел',
        0:'решка'
          }
    
    logging.info(f'выпал {heads_or_tails_dict[rand_int]}')



with DAG("n-chumurov_practice3",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['n-chumurov']
         ) as dag:
    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
            task_id='echo_ds',
            bash_command='echo {{ ds }}'
        )

    heads_or_tails_rate = PythonOperator(
            task_id='heads_or_tails',
            python_callable=heads_or_tails,
            op_args=[randint(0,1)]
            
        )

 

    dummy >> [echo_ds, heads_or_tails_rate]