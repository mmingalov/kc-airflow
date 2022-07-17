import datetime as dt
import random

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
import pendulum


DEFAULT_ARGS = {
    'owner': 'pinchuk.ilya.94@yandex.ru',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': dt.timedelta(minutes=1),
    'email_on_retry': False,
    'email_on_failure': False
}


POSTGRES_HOOK = PostgresHook(postgres_conn_id='conn_greenplum')


def push_to_xcom(**kwargs) -> None:
    connection = POSTGRES_HOOK.get_conn()
    cursor = connection.cursor('named_cursor_name')
    execution_date = pendulum.parse(kwargs['a-gajdabura'])
    query = f'SELECT heading FROM articles WHERE id = {execution_date.day_of_week}'
    print(f'Performing SQL query: {query}')
    cursor.execute(query)

    string_execution_date = execution_date.format('dddd').lower()

    try:
        data = cursor.fetchone()[0]
        task_instance = kwargs['task_instance']
        task_instance.xcom_push(value=data, key=f'{string_execution_date}_article_heading')
    except TypeError as e:
        print(f'No data for {string_execution_date}')
        raise e


def select_random() -> str:
    return random.choice(['bash_task', 'python_task'])


with DAG(
    dag_id='pinchuk_ilya_94_dag',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval='@daily',
    tags=['karpov']
) as dag:
    input_task = BashOperator(
        task_id='hello_task',
        bash_command='echo "Hello from the first task!"'
    )

    python_task = PythonOperator(
        task_id='python_task',
        python_callable=lambda : print('Hello from the branched python task!')
    )

    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "Hello from the branched bash task!"'
    )

    def select_task():
        return random.choice([python_task.task_id, bash_task.task_id])

    branch_operator = BranchPythonOperator(
        task_id='branch_operator_task',
        python_callable=select_task
    )

    xcom_task = PythonOperator(
        task_id='xcom_task',
        python_callable=push_to_xcom
    )

    input_task >> xcom_task >> branch_operator >> [python_task, bash_task]
