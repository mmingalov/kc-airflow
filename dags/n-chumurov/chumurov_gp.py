"""
DAG для загрузки данных из GreenPlum по расписанию пн-сб

"""

from airflow import DAG
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator

import logging
from datetime import datetime

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'n-chumurov',
    'depends_on_past': True
}

day_of_week = "{{ execution_date.strftime('%w') }}"

with DAG('n-chumurov_gp_les4',
    schedule_interval='0 0 * * 1-6',        
    default_args=DEFAULT_ARGS,    
    max_active_runs=1,
    tags=['n-chumurov'],
) as dag:


    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}'
    )

    def gp_hook_funck(date):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {date}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        #one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info(query_res)

    gp_hook = PythonOperator(
        task_id='gp_hook_funck',
        python_callable=gp_hook_funck,
        op_args=[day_of_week]
        )
    

    dummy >> [echo_ds, gp_hook]
    

    


