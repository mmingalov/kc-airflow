"""
Самый обычный-преобычный даг-чувак
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import csv

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import BranchPythonOperator

import random

DEFAULT_ARGS = {
    'owner': 'a.chirkovskij-2',
    'start_date': days_ago(2),
    'poke_interval': 600,
}


with DAG("a.chirkovskij-2_1",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a.chirkovskij-2']
         ) as dag:

    start = DummyOperator(task_id='start')

    saluton_sh = BashOperator(
        task_id='saluton_sh',
        bash_command='echo "Saluton! Hodiaux estas {{ a-gajdabura }}!"'
    )

    def select_lingvo_f():
        return random.choice(['saluton_py', 'hello_py', 'lambda_py'])
       
    select_lingvo = BranchPythonOperator(
        task_id='select_lingvo',
        python_callable=select_lingvo_f
    )
    
    def saluton_py_f():
        w1 = ['Saluton', 'Hallo', 'Bienvenite']
        w2 = ['Kastrulon', 'Kaserolen', 'Amikoj']
        logging.info(f'{random.choice(w1)} {random.choice(w2)}!')
        
    def hello_py_f():
        w1 = ['Hello', 'Greetings', 'Have an ICE day']
        w2 = ['my Friends', 'Liebe Freunde', 'Hundarkaroj']
        logging.info(f'{random.choice(w1)} {random.choice(w2)}!')

    saluton_py = PythonOperator(
        task_id='saluton_py',
        python_callable=saluton_py_f
    )
    
    hello_py = PythonOperator(
        task_id='hello_py',
        python_callable=hello_py_f
    )
    
    lambda_py = PythonOperator(
        task_id='lambda_py',
        python_callable=lambda: logging.info('Saltation Hundarete Lambdissimo')
    )
    
    lambda_dummy = DummyOperator(task_id='lambda_dummy')
    
    bonvenon_sh = BashOperator(
        task_id='bonvenon_sh',
        bash_command='echo "Bonvenon!"'
    )
    
    
    start >> saluton_sh >> select_lingvo >> [saluton_py, hello_py >> bonvenon_sh, lambda_py >> lambda_dummy] 
