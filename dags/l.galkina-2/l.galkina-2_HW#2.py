"""
Даг - ДЗ для 4-го урока
"""

import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'owner': 'l.galkina-2',
    'poke_interval': 600,
    'retries': 1,
    'start_date': days_ago(2),
    'depends_on_past': True,
    'email': ['liudmila.galkina@gmail.com'],
    'email_on_failure': True
}


def check_integrity():
    logging.info("Integrity was checked for transformed data from source")


def transform(source_id):
    logging.info("Data from source " + str(source_id) + " was transformed")


def get_heading_from_gp(execution_date):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor("named_cursor_name")
    cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(execution_date.weekday() + 1))
    query_res = cursor.fetchall()
    return query_res


with DAG(
        dag_id='l.galkina-2_gp_dag',
        schedule_interval='0 0 * * 1-6',
        tags=['karpov', 'greenplum', 'l.galkina'],
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

    gpHeadingOp = PythonOperator(
        task_id='get_heading_from_gp',
        python_callable=get_heading_from_gp,
        provide_context=True
    )

    createReportOp >> gpHeadingOp >> mergeDateOp

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
