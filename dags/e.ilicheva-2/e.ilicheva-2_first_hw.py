"""
Homework, part 1
"""

from airflow import DAG
from datetime import datetime
import random

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash_operator import BashOperator

DEFAULT_ARGS = {
    'start_date': datetime(2021, 11, 18),
    'end_date': datetime(2021, 11, 30),
    'owner': 'e.ilicheva-2',
    'poke_interval': 600
}

with DAG(dag_id="e.ilicheva-2-1st_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         tags=['e.ilicheva-2', 'first_homework']) as dag:
    start = DummyOperator(task_id='start')

    def choose_task():
        x = random.randint(1, 2)
        if x == 1:
            return 'echo_1'
        else:
            return 'echo_2'


    branch_task = BranchPythonOperator(task_id='branch_task',
                                       dag=dag,
                                       python_callable=choose_task,
                                       provide_context=True)

    echo_1 = BashOperator(
        task_id='echo_1',
        bash_command='echo {{ a-gajdabura }}'
    )

    echo_2 = BashOperator(
        task_id='echo_2',
        bash_command='echo {{ ds_nodash }}'
    )

    end = DummyOperator(task_id='end')

    start >> branch_task >> [echo_1, echo_2] >> end
