import logging
import random

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago

from airflow import DAG

DEFAULT_ARGS = {
    "owner": "msamsonov",
    "start_date": days_ago(2),
}

with DAG(
    "msamsonov_example",
    schedule_interval="@daily",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["msamsonov"],
) as dag:

    def select_randomly():
        return random.choice(["print_hello_python", "print_hello_bash"])

    select_randomly_python_or_bash = BranchPythonOperator(
        task_id="select_randomly_python_or_bash", python_callable=select_randomly
    )

    def print_hello_python():
        logging.info("Hello from python")

    hello_python = PythonOperator(
        task_id="print_hello_python", python_callable=print_hello_python
    )

    hello_bash = BashOperator(
        task_id="print_hello_bash",
        bash_command='echo "Hello from bash"',
    )

    end = DummyOperator(task_id="end", trigger_rule="one_success")

    select_randomly_python_or_bash >> [hello_python, hello_bash] >> end
