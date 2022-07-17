"""
4 урок Задание
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(5),
    'owner': 'm-frantsev-4',
    'poke_interval': 600
}

with DAG("m-frantsev-4",
        schedule_interval='0 0 * * 1-6',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['m-frantsev-4']
        ) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )


    def get_connect(**kwargs):

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum') # инициализируем хук
        conn = pg_hook.get_conn() # берём из него соединение
        cursor = conn.cursor() # и именованный (необязательно) курсор

        # ставим день
        wd = datetime.datetime(2022, 3, 11).weekday()+1 #понедельник это 0

        # # исполняем sql
        cursor.execute(f'SELECT heading FROM articles WHERE id = {wd}')
        one_string = cursor.fetchall()

        # логируем
        logging.info('---------------------------------------------------')
        logging.info(f'weekday: {datetime.datetime(2022, 3, 11)}')
        logging.info(f'weekday: {wd}')
        logging.info(f'value: {one_string}')
        logging.info('---------------------------------------------------')

        kwargs['ti'].xcom_push(value=one_string, key='heading')

    push_connect = PythonOperator(
        task_id='push_connect',
        python_callable=get_connect,
        dag=dag
    )

    dummy >> echo_ds >> push_connect