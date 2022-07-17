"""
Дагуся, тащущий данные из гринплама по дню недели.
log_weekday -- парсит время выполнения, сохраняет день недели в X-COM
not_sunday -- возвращает False, если воскресенье (по воскресеньям не работаем)
fetch_data -- тянет данные из таблицы, соответствующие дню из X-COM-а, результат в X-COM.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import csv
from datetime import datetime

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook

import random

DEFAULT_ARGS = {
    'owner': 'a.chirkovskij-2',
    'start_date': days_ago(2),
    'poke_interval': 600,
}


with DAG("a.chirkovskij-2_2",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a.chirkovskij-2']
         ) as dag:

    start = DummyOperator(task_id='start')

    def log_weekday_f(**kwargs):
        exec_day = datetime.strptime(kwargs['templates_dict']['a-gajdabura'], '%Y-%m-%d').weekday()
        #logging.info(kwargs)
        kwargs['ti'].xcom_push(value=exec_day, key='dow') #Складываем в X-com
        logging.info('Execution time: ' + kwargs['templates_dict']['a-gajdabura'])
        logging.info('Day of the week: ' + ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][exec_day])
    
    log_weekday = PythonOperator(
        task_id='log_weekday',
        python_callable=log_weekday_f,
        templates_dict={'a-gajdabura': '{{ a-gajdabura }}'},
        provide_context=True
    )
    
    def not_sunday_f(**kwargs):
        exec_day = kwargs['ti'].xcom_pull(task_ids='log_weekday', key='dow')
        return exec_day != 6
    
    not_sunday = ShortCircuitOperator(
        task_id='not_sunday',
        python_callable=not_sunday_f,
        provide_context=True
    )
         
    def fetch_data_f(**kwargs):
        exec_day = kwargs['ti'].xcom_pull(task_ids='log_weekday', key='dow')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("der_cursor")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {exec_day}')  # исполняем sql
        query_res = cursor.fetchall()
        return query_res
        
    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_f,
        provide_context=True
        )

    
    start >> log_weekday >> not_sunday >> fetch_data
