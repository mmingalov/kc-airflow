"""
Из Geenplum с понедельника по субботу достаем heading из таблицы articles
"""
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'a-svirin',
    'poke_interval': 10,
    'email_on_failure': True,
    'retries': 3}

with DAG("svirin_test",
    schedule_interval='0 0 * * 1-6',
    max_active_runs=1,
    tags=['a-svirin'],
    default_args=DEFAULT_ARGS) as dag:

    def get_data_from_greenplum_and_write_to_logs(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор

        today = kwargs['templates_dict']['today']
        day_of_week = datetime.strptime(today, '%Y-%m-%d').weekday() + 1

        if day_of_week != 7:
            query = 'SELECT heading FROM articles WHERE id = {}'.format(day_of_week)
            result = cursor.execute(query)  # исполняем sql
            query_result = cursor.fetchall()  # полный результат

            logging.info('--------')
            logging.info('Date: {}, Weekday: {}'.format(today, day_of_week))
            logging.info('Value: {}'.format(query_result))
            logging.info('--------')

    get_string_for_today = PythonOperator(
        task_id='get_string_for_today',
        python_callable=get_data_from_greenplum_and_write_to_logs,
        templates_dict={'today': '{{ ds }}'},
        dag=dag)

    #get_data = PythonOperator(
    #    task_id='get_logs',
    #    python_callable=get_data_from_greenplum_and_write_to_logs,
    #    dag=dag)