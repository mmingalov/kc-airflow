from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    'owner': 'l-parubets',
    'start_date': days_ago(0),
    'depends_on_past': False,
}

with DAG(
    'l-parubets-3',
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    catchup=False,
    tags=['l-parubets']
) as dag:
    t1 = BashOperator(
        task_id='echo_hi',
        bash_command='echo "Hello"',
    )
    t2 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )
    t1 >> t2

