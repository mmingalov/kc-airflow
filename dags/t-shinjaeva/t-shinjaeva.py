"""
простой даг
"""
from airflow import DAG
import logging
import datetime as dt
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_ARGS = {
    'start_date': dt.datetime(2022, 2, 25, 0, 0, 0),
    'end_date': dt.datetime(2022, 3, 10, 0, 0, 0),
    'owner': 't-shinjaeva',
    'poke_interval': 600
}

with DAG("t-shinjaeva",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['t-shinjaeva']
) as dag:

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}'
    )

    def exec_date_func(weekday, **kwargs):
        logging.info('--------------------------------')
        logging.info('context, {{logical_date}}: ' + str(kwargs['logical_date']))
        logging.info('context, {{ds}}: ' + kwargs['ds'])
        logging.info('weekday, {{ds}}: ' + weekday)
        logging.info('--------------------------------')

    exec_date = PythonOperator(
        task_id='exec_date',
        python_callable=exec_date_func,
        op_args=['{{logical_date.isoweekday()}}'],
        provide_context=True
    )

    empty_task_1 = DummyOperator(task_id='date_in_range', dag=dag)
    empty_task_2 = DummyOperator(task_id='date_outside_range', dag=dag)

    cond1 = BranchDateTimeOperator(
        task_id='datetime_branch',
        follow_task_ids_if_true=['date_in_range'],
        follow_task_ids_if_false=['date_outside_range'],
        target_upper=dt.datetime(2022, 3, 1, 0, 0, 0),
        target_lower=dt.datetime(2020, 3, 14, 0, 0, 0),
        dag=dag,
    )

    echo_ds >> exec_date >> cond1 >> [empty_task_1, empty_task_2]