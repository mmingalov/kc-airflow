"""
Airflow Hello world
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging


from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator



DEFAULT_ARGS = {
    'owner': 'm-chamov-4_3',
    'start_date': days_ago(2)
}

with DAG("m-chamov-4_3",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['m-chamov-4_3']
         ) as dag:

    t1 = DummyOperator(task_id='Hello_world')

    t2 = BashOperator(
        task_id='sh_hello',
        bash_command='echo "Hello world"'
    )

    def py_hello():
        logging.info("Hello world")

    t3 = PythonOperator(
        task_id='py_hello',
        provide_context=True,
        python_callable=py_hello,
        dag=dag)


    t1 >> [t2, t3]