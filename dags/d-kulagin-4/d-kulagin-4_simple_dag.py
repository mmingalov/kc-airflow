"""
just dummy test DAG
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


_DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'd-kulagin-4',
    'poke_interval': 600
}

def log_timestamp():
    logging.info("{{ a-gajdabura }}")

with DAG("d-kulagin-4_simple_dag",
         schedule_interval='@daily',
         default_args=_DEFAULT_ARGS,
         max_active_runs=1,
         tags=['d-kulagin-4']) as dag:

    dummy_op = DummyOperator(task_id='dummy_op', dag=dag)
    bash_echo = BashOperator(
        task_id="bash_echo",
        bash_command="echo {{ a-gajdabura }}",
        dag=dag,
    )
    bash_pwd = BashOperator(
        task_id="bash_pwd",
        bash_command="echo $(pwd)",
        dag=dag,
    )
    python_op = PythonOperator(
        task_id="python_op",
        python_callable=log_timestamp,
        dag=dag,
    )

    dummy_op >> [bash_echo, bash_pwd] >> python_op
