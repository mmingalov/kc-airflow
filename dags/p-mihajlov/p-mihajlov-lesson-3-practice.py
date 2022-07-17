"""
Даг показывает дни до Нового Года
выводит текущую дату (BashOperator),
выводит приветствие и день недели (PythonOperator),
выводит количество дней до ближайшего Нового Года (PythonOperator)
"""

from airflow import DAG
from datetime import datetime
import logging
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

DEFAULT_ARGS = {
    'start_date': datetime(2022, 4, 10),
    'owner': 'p-mihajlov',
    'poke_interval': 600
}

with DAG("p-mihajlov-lesson-3-practice",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['p-mihajlov', 'lesson-3']
         ) as dag:

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    weekDays = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
    today = datetime.now()
    day_today = datetime.weekday(datetime.now())


    def greeting_func():
        if day_today in [0, 1, 2, 3, 4]:
            logging.info(f"Hello dear friend! Today is crappy {weekDays[day_today]}")
        else:
            logging.info(f"Hello dear friend! Today is sunny {weekDays[day_today]}")

    greeting = PythonOperator(
        task_id='greeting',
        python_callable=greeting_func,
        dag=dag
    )

    def days_till_new_year():
        next_new_year = datetime(today.year + 1, 1, 1)
        days = (next_new_year - datetime.now()).days + 1
        logging.info(f"Did you behave well yesterday? There are {days} days left until the New Year.")

    new_year_task = PythonOperator(
        task_id='new_year_task',
        python_callable=days_till_new_year,
        dag=dag
    )

    echo_ds >> greeting >> new_year_task

dag.doc_md = __doc__
echo_ds.doc_md = """Пишет в лог execution_date"""
greeting.doc_md = """Пишет в лог приветствие и день недели"""
new_year_task.doc_md = """Пишет в лог количество дней до Нового Года"""
