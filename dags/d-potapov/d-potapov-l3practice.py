"""
d-potapov
Практика урока 3
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import random

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'd-potapov',
    'poke_interval': 600
}

with DAG("d-potapov_practice_l3",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['d-potapov']
) as dag:

# Dummy
    initial_task = DummyOperator(task_id="initial_task")

# Bash 1
    echo_templates = BashOperator(
        task_id='echo_templates',
        bash_command="echo 'Execution date:' {{ execution_date }}",
        dag=dag
    )

# Bash 2
    echo_server_info = BashOperator(
        task_id = 'echo_server_info',
        bash_command = 'whoami; pwd;'
    )

# BranchPythonOperator

    def get_next_tasks():
        return random.sample(set(['var1','var2','var3']),2)

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=get_next_tasks
    )

    var1 = DummyOperator(task_id="var1")
    var2 = DummyOperator(task_id="var2")
    var3 = DummyOperator(task_id="var3")

    var1_ = DummyOperator(task_id="var1_")
    var2_ = DummyOperator(task_id="var2_")
    var3_ = DummyOperator(task_id="var3_")


    initial_task >> echo_templates >> echo_server_info >> branch_task >> [var1,var2,var3]
    var1.set_downstream(var1_)
    var2.set_downstream(var2_)
    var3.set_downstream(var3_)