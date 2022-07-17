"""First for kc DAG with DummyOperator, BashOperator, BranchPythonOperator"""

import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago


default_args = {
    'start_date': days_ago(2),
    'owner': 'o-likiy',
    'poke_interval': 600
}


def print_from_python(text):
    print(text)


def branch_selection(**kwarg):
    if kwarg['execution_date'].weekday() in [0, 1, 2, 3, 4]:
        return 'workdays_task'
    else:
        return 'weekends_task'


def branch_workdays(dt, day_of_the_week):
    print(dt.date(), day_of_the_week[dt.weekday()])


def branch_weekends(dt, day_of_the_week):
    print(dt.date(), day_of_the_week[dt.weekday()])


with DAG(
    dag_id='o-likiy_first_dag',
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@daily",
    tags=['o-likiy'],
) as dag:

    start_task = DummyOperator(task_id='start_task')

    bash_task = BashOperator(task_id='bash_task',
                             bash_command="echo 'Hello World from BashHOperator!'")

    python_task = PythonOperator(task_id='python_task',
                                 python_callable=print_from_python,
                                 op_kwargs={'text': 'Hello World from PythonOperator!'})

    select_branch_task = BranchPythonOperator(task_id='select_branch_task',
                                              python_callable=branch_selection)

    workdays_task = PythonOperator(task_id='workdays_task',
                                   python_callable=branch_workdays,
                                   op_kwargs={'dt': datetime.datetime.now(),
                                              'day_of_the_week': ['Monday', 'Tuesday', 'Wednesday', 'Thursday',
                                                                  'Friday', 'Saturday', 'Sunday']},)

    weekends_task = PythonOperator(task_id='weekends_task',
                                   python_callable=branch_weekends,
                                   op_kwargs={'dt': datetime.datetime.now(),
                                              'day_of_the_week': ['Monday', 'Tuesday', 'Wednesday', 'Thursday',
                                                                  'Friday', 'Saturday', 'Sunday']},)

    end_task = DummyOperator(task_id='end_task',
                             trigger_rule=TriggerRule.NONE_FAILED)

    start_task >> bash_task >> python_task >> select_branch_task >> [workdays_task, weekends_task] >> end_task
