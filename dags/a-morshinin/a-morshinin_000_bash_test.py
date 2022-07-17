from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

test_dag_bash = DAG(
    "a-morshinin_000_dag_bash",
    start_date=days_ago(0, 0, 0, 0),
    tags = ['a-morshinin']
)

pwd = BashOperator(
    bash_command="pwd",
    dag=test_dag_bash,
    task_id='pwd'
)

ls = BashOperator(
    bash_command="ls",
    dag=test_dag_bash,
    task_id='ls'
)

cd = BashOperator(
    bash_command="cd",
    dag=test_dag_bash,
    task_id="cd"
)

pwd >> ls >> cd
