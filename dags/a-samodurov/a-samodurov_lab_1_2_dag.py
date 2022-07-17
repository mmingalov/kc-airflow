from datetime import timedelta, datetime
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'a-samodurov',
    'start_date': datetime(2022, 3, 1),
    'end_date':  datetime(2022, 3, 14),
    'depends_on_past': True
}

weekday = "{{ execution_date.strftime('%w') }}"

with DAG(
    dag_id='a_samodurov_gp_hook_dag',
    description='Тестим airflow',
    default_args=default_args,
    schedule_interval='0 9 * * 1-6',
    max_active_runs=1,
    tags=['a-samodurov'],
) as dag:

    def gp_hook_execute_(dt) -> None:
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {dt}')
        query_res = cursor.fetchall()
        logging.info(query_res)

    gp_hook_execute = PythonOperator(
        task_id='gp_hook_execute_',
        python_callable=gp_hook_execute_,
        op_args=[weekday]
    )
    bash_hello = BashOperator(
        task_id='echo_hello_',
        bash_command='echo Hello from airflow'
    )
    dummy = DummyOperator(
        task_id='dummy_task_'
    )

gp_hook_execute >> [bash_hello, dummy]
