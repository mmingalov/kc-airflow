"""
Этот даг вычитывает текущую дату и на основании нее делает расчет количества дней до Нового года (31 декабря 2021)
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator

DEFAULT_ARGS = {
    'start_date': days_ago(0),
    'owner': 'arhipova',
    'poke_interval': 600
}

with DAG("arhipova_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['karpov']
         ) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    def days_before_NY_func():
        today = datetime.date.today()
        print(f"сегодня {today}")
        NY_day = datetime.date(2021, 12, 31)
        delta = NY_day - today
        print(f"до Нового Года осталось {delta.days} дней")


    days_before_NY = PythonOperator(
        task_id='days_before_NY',
        python_callable=days_before_NY_func
    )

    start >> days_before_NY >> end
