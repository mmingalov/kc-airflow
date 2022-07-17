"""
Тестовый даг.
**описание где-то в аирфлоу**
* поддерживает MD ?
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'Poroshin'
}

dag = DAG(
    dag_id='just_testing',
    schedule_interval='*/5 * * * *',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['sporoshin']
)

dummy = DummyOperator(task_id='dummy', dag=dag)

echo = BashOperator(
    task_id='command_echo',
    bash_command='echo {{ s-poroshin }}',
    dag=dag
)

def main():
    print('From python {{ s-poroshin }}')

python = PythonOperator(
    task_id='python_print',
    python_callable=main,
    dag=dag
)

dummy >> echo >> python
