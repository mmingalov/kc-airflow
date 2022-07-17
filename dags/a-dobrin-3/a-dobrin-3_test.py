"""
Простой даг для пробы
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-dobrin-3',
    'poke_interval': 600
}

with DAG("a-dobrin-3_test_dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['dobrin']
          ) as dag:

    dummy = DummyOperator(task_id='dummy_da')

    echo = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ a-gajdabura }}',
        dag=dag
    )

    def add_num_func(**kwargs):
        answ = kwargs['templates_dict']['num1'] + kwargs['templates_dict']['num2']
        logging.info(answ)

    add_num = PythonOperator(
        task_id='log_ds',
        python_callable=add_num_func,
        templates_dict={'num1': 1, 'num2': 6},
        dag=dag
    )

    dummy >> [echo, add_num]


