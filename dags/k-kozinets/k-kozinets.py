from datetime import datetime
from textwrap import dedent
from airflow.models.dag import dag
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
import logging

params_str = dedent("""
    logical_date: {{ logical_date }}
    dag_id: {{ conf.dag_id }}
    run_id: {{ conf.run_id }}
    """)

DEFAULT_ARGS = {
    'owner': 'k-kozinets',
    'email': ['kosotis@yandex.ru'],
    'start_date': datetime(2022, 6, 5),
    'email_on_failure': True,
    'email_on_retry': True,
    'depends_on_past': False,
    'tags': ['dwopek']
}


def get_params(param):
    logging.info(param)


@dag(
    # start_date=pendulum.now().subtract(days=2).diff_for_humans(),
    start_date=datetime(2022, 6, 6),
    dag_id='dwopek_dag',
    schedule_interval='@once',
    default_args=DEFAULT_ARGS,
    max_active_runs=1
)
def generate_dag():
    get_logs = PythonOperator(
        task_id='get_logs',
        python_callable=get_params,
        op_args=[params_str]
    )
    get_path = BashOperator(
        task_id='path',
        bash_command="echo \"PWD: '$PWD'\""

    )
    end = DummyOperator(
        task_id='end',
        trigger_rule='all_success'

    )
    [get_logs, get_path] >> end


dag = generate_dag()
