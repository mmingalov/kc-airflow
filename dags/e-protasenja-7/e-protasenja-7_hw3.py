"""
Скрипт по заданию 3
"""

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from datetime import datetime
from datetime import timedelta
import logging

DEFAULT_ARGS = {
    'owner': 'e-protasenja-7',
    'poke_interval': 600,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
}


with DAG(
    dag_id='e-protasenja-7_data_from_gp-2',
    schedule_interval='0 4 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['e-protasenja-7']
) as dag:

    start = DummyOperator(task_id='start')

    def is_target_weekday_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day != 6

    target_weekday = ShortCircuitOperator(
        task_id='is_target_weekday',
        python_callable=is_target_weekday_func,
        op_kwargs={'execution_dt': '{{ ds }}'},
        dag=dag
    )

    def select_from_gp_func(execution_dt):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        logging.info(execution_dt)

        cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(
            datetime.strptime(execution_dt, '%Y-%m-%d').weekday() + 1))
        query_res = cursor.fetchall()

        logging.info(query_res)

    select_from_gp = PythonOperator(
        task_id='select_data_from_gp',
        python_callable=select_from_gp_func,
        op_kwargs={'execution_dt': '{{ ds }}'},
        dag=dag
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule='all_success'
    )

    start >> target_weekday >> select_from_gp >> end
