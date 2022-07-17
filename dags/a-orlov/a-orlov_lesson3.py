from airflow import DAG

from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

from datetime import datetime

DEFAULT_ARGS = {
    'owner': 'Lexx Orlov',
    'start_date': datetime(2022,6,1)
}

dag = DAG('the_dag',
          description='My first DAG',
          default_args=DEFAULT_ARGS,
          schedule_interval='@once',
          max_active_runs=1,
          tags=['lexxo']
          )

start = DummyOperator(
    task_id='dag_start',
    dag=dag
)


@task(task_id='weekend_greet', dag=dag)
def weekend_greet_func():
    print('Oh, by the way, it\'s weekend! Time to PARTAAAYYY!!!')


@task(task_id='weekday_greet', dag=dag)
def weekday_greet_func():
    print('It\'s weekday now, so get to work, you lazy ass!')


def is_weekend():
    today = datetime.today()
    weekday = today.weekday()
    return weekday > 4


def weekend_route():
    if is_weekend():
        return 'weekend_greet'
    else:
        return 'weekday_greet'


branch_dt = BranchPythonOperator(
    task_id='branch_weekday',
    python_callable=weekend_route,
    dag=dag
)


bash_cmd = BashOperator(task_id='bash_cmd',
                        bash_command='ls -la',
                        dag=dag)


def hello_world():
    print('Welcome to this shitty world, my friend!')


py_func = PythonOperator(task_id='python_func',
                         python_callable=hello_world,
                         trigger_rule='none_failed',
                         dag=dag)


start >> branch_dt >> [weekend_greet_func(), weekday_greet_func()] >> py_func >> bash_cmd


dag.doc_md = __doc__

weekday_greet_func.doc_md = 'Пишет в stdout, если рабочий день'
weekend_greet_func.doc_md = 'Пишет в stdout, если викенд'
py_func.doc_md = 'Пишет что-то в stdout'
bash_cmd.doc_md = 'Ранит ls -la в консоли'
