"""
Мой первый даг
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
from datetime import datetime

from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': '2022-03-01',
    'end_date': '2022-03-14',
    'owner': 't-tihomirov',
    'poke_interval': 600
}

with DAG("t-tihomirov-lesson4",
        schedule_interval='0 0 * * 1-6',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['t-tihomirov']
        ) as dag:

    def read_articles(ds):
        logging.info(f'Ds original = {str(ds)}')
        ds = datetime.strptime(ds,'%Y-%m-%d')
        ds = ds.isoweekday()
        logging.info(f'Ds weekday = {str(ds)}')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("mycursor")  # и именованный (необязательно) курсор
        sql = f'SELECT heading FROM articles WHERE id = {ds}'
        logging.info(sql)
        cursor.execute(sql)  # исполняем sql
        try:
            one_string = cursor.fetchone()[0]
            logging.info(one_string) # если вернулось единственное значение
        except Exception as e:
            logging.info(f'Error in fetchone : {str(e)}')
        
        try:
            all_headings = cursor.fetchall()[0]
            logging.info(all_headings)
        except Exception as e:
            logging.info(f'Error in fetchall : {str(e)}')

    read_operator = PythonOperator(
        task_id='read_operator',
        python_callable=read_articles,
        dag=dag
    )

    read_operator
