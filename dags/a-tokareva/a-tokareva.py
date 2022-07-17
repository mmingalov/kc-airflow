"""
Мой второй DAG
"""

# чтобы определить DAG
from airflow import DAG
# чтобы определить расписание запуска
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

# эти переменные присваиваются дагу и наследуются всеми его тасками
DEFAULT_ARGS = {
    'owner': 'a-tokareva',
    'start_date': days_ago(2)
}

dag = DAG(
    'a-tokareva',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS
)

dummy = DummyOperator(
    task_id='dummy',
    dag=dag
)

our_date = days_ago(2).strftime("%d/%m/%Y")

echo_ds = BashOperator(
    task_id='echo_ds',
    bash_command=f'curl https://www.cbr.ru/scripts/XML_daily.asp?date_req={our_date} | iconv -f Windows-1251 -t UTF-8',
    dag=dag
)

dummy >> echo_ds
