from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
import logging

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_ARGS = {
    'start_date': days_ago(0),
    'owner': 'an-shishkov',
    'poke_interval': 600
}

with DAG(
    dag_id='avs_test',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['an-shishkov']
) as dag:
    
    dummy_task = DummyOperator(task_id='dummy_task')
    
    echo_task = BashOperator(
        task_id='echo_task',
        bash_command='echo "hello, airflow! it is bash"',
        dag=dag
    )
    
    def hello_airflow_func():
        msg = 'hello, airflow! it is python'
        logging.info(msg)
        print(msg)
    
    hello_airflow_task = PythonOperator(
        task_id='python_task_1',
        python_callable=hello_airflow_func,
        dag=dag
    )
    
    def date_func():
        msg = datetime.today().strftime('%Y-%m-%d')
        logging.info(msg)
        print(msg)
    
    date_task = PythonOperator(
        task_id='python_task_2',
        python_callable=date_func,
        dag=dag
    )
    
    dummy_task >> [echo_task, hello_airflow_task, date_task]
