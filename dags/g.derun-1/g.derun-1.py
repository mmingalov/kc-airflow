"""
Тут будет некая важная информация для команды
Это третий заход на запуск дага.
"""


from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def print_smth():
    """
    Print first part
    """
    print('Hello, ')


with DAG(dag_id='rpuropuu_dag_001',
         default_args={'owner': 'rpuropuu', 'retries': 5},
         schedule_interval='@daily', # schedule_interval="0 0 * * 1-6"
         max_active_runs=1,
         start_date=days_ago(1),
         tags=['exercise_001']
    ) as dag:

    t_1 = PythonOperator(
        task_id='t_1',
        python_callable=print_smth
    )

    t_2 = BashOperator(
        task_id='t_2',
        bash_command='echo "Dina! =)"'
    )

    t_3 = DummyOperator(task_id='t_3')

    t_1 >> t_2 >> t_3
