"""
Даг - ДЗ для 3-го урока
"""

import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'owner': 'liudmila.galkina',
    'poke_interval': 600,
    'retries': 1,
    'start_date': days_ago(2),
    'depends_on_past': True,
    'email': ['liudmila.galkina@gmail.com'],
    'email_on_failure': True
}


def transform(source_id):
    logging.info("Data from source " + str(source_id) + " was transformed")


def check_integrity():
    logging.info("Integrity was checked for transformed data from source")


with DAG(
        dag_id='l.galkina-2_simple_dag',
        schedule_interval='@daily',
        tags=['karpov', 'simple_tag'],
        max_active_runs=1,
        default_args=DEFAULT_ARGS
) as dag:
    createReportOp = BashOperator(
        task_id='create_report',
        bash_command='echo "Create report"'
    )

    mergeDateOp = DummyOperator(
        task_id='merge_data'
    )

    sourceOps = []
    for i in range(3):
        sourceOp = BashOperator(
            task_id='get_data_from_source_' + str(i),
            bash_command='echo "Get report data from source"'
        )

        transformOp = PythonOperator(
            task_id='transform_' + str(i),
            op_args=[i],
            python_callable=transform
        )

        sourceOp >> transformOp >> mergeDateOp
        sourceOps.append(sourceOp)

    createReportOp >> sourceOps

    checkIntegrityOp = ShortCircuitOperator(
        task_id='check_integrity',
        python_callable=lambda: True
    )

    loadOp = DummyOperator(
        task_id='load'
    )

    saveReportOp = DummyOperator(
        task_id='save_report'
    )

    mergeDateOp >> checkIntegrityOp >> loadOp >> saveReportOp
