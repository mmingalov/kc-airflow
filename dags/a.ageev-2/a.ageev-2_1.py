import logging
import os
import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'owner': 'ageev',
    'queue': 'ageev_queue',
    'pool': 'user_pool',
    'email': ['ageev_alex@mail.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'wait_for_downstream': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(seconds=20),
    'priority_weight': 10,
    'start_date': datetime.datetime(2021, 1, 1),
    'end_date': datetime.datetime(2025, 1, 1),
    'sla': datetime.timedelta(hours=2),
    'execution_timeout': datetime.timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule':  'all_success'
}



dag = DAG(
    "a.ageev-2_dag_1",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

def f_t1():
    logging.info('hi')
    logging.info(f"{os.getcwd()}")
    logging.info(f"{os.listdir()}")

t1 = PythonOperator(
    task_id='t1',
    python_callable=f_t1,
    dag=dag
)

start >> t1
t1 >> end

