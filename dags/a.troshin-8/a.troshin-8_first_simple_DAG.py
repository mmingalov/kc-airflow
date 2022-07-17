"""
First simple DAG
"""
from airflow import DAG
import logging
from datetime import datetime

from airflow.operators.datetime import BranchDateTimeOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 4, 1),
    'owner': 'a.troshin-8',
    'poke_interval': 600
}

with DAG("a.troshin-8_DAG_01_04",
    schedule_interval='15 12 * * *',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a.troshin-8']
) as dag:

    april_fools_day_today = BashOperator(
        task_id='april_fools_day_today',
        bash_command='echo "today {{ ds }}  Happy April Fool s Day"',
        dag=dag
    )

    def april_fools_day_left_func():
        str_ = ''' Today {cur_date}. April Fool's Day was {delta} days ago :('''
        cur_date = datetime.today()
        delta = (cur_date - datetime(2022, 4, 1)).days
        logging.info(str_.format(cur_date=cur_date.date(), delta=delta))

    april_fools_day_left = PythonOperator(
        task_id='april_fools_day_left',
        python_callable=april_fools_day_left_func,
        dag=dag
    )
    date_condition = BranchDateTimeOperator(
        task_id='date_condition',
        follow_task_ids_if_true=['april_fools_day_today'],
        follow_task_ids_if_false=['april_fools_day_left'],
        target_lower=datetime(2022, 4, 1, 0, 0, 0),
        target_upper=datetime(2022, 4, 1, 23, 59, 59),
        dag=dag
    )

    date_condition >> [april_fools_day_today, april_fools_day_left]
