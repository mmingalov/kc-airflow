import logging
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.dates import days_ago

my_id = 'e-netrebko-7'

default_args = {
    'owner': my_id,
    'depends_on_past': False,
    'start_date': days_ago(1),
    'catchuo': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


def print_str(name: str) -> None:
    logging.info(f'Hello, {name}!')


def choose_calculate_or_not(**kwargs) -> str:
    if datetime.strptime(kwargs['ds'], '%Y-%m-%d').weekday() < 5:
        return 'print_str_bash'
    return 'print_str_python'


with DAG(
    dag_id=f'{my_id}_1st_hw',
    default_args=default_args,
    schedule_interval=None,
) as dag:

    start_flow = DummyOperator(task_id='start_flow')
    end_flow = DummyOperator(task_id='end_flow', trigger_rule='all_done')

    weekends_branch = BranchPythonOperator(
        task_id='weekends_branch',
        python_callable=choose_calculate_or_not,
        provide_context=True,
    )
    print_str_bash = BashOperator(
        task_id='print_str_bash',
        bash_command=f'echo "Hello, {my_id}!"',
    )
    print_str_python = PythonOperator(
        task_id='print_str_python',
        python_callable=print_str,
        op_kwargs={
            'name': my_id,
        },
    )

    start_flow >> weekends_branch >> [print_str_bash, print_str_python] >> end_flow
