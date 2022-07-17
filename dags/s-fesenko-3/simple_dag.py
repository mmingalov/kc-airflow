"""самый простой даг"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(4),
    'owner': 's-fesenko-3',
    'poke_interval': 600
}

with DAG("lesson_3",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['s-fesenko-3']
          ) as dag:

    dummy = DummyOperator(task_id='dummy')

    echo_ds = BashOperator(
        task_id = 'echo_ds',
        bash_command = 'echo {{ a-gajdabura }}',
        dag = dag
    )


    def log_ds_func(**kwargs):
        ds = kwargs['templates_dict']['a-gajdabura']
        logging.info(ds)

    log_ds = PythonOperator(
        task_id = 'log_ds',
        python_callable = log_ds_func,
        templates_dict = {'a-gajdabura': '{{ a-gajdabura }}'},
        dag = dag
    )

    dummy >> [echo_ds, log_ds]