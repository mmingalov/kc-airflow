"""
Даг для урока 3 и 4. Берем данные из GreenPlum и выводим в лог. Работает без вскр с 1 марта по 14 марта.
"""
from airflow import DAG
import logging
import textwrap
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 2),
    'end_date': datetime(2022, 3, 15),
    'owner': 'e-pavlov-8',
    'poke_interval': 600
}

dag = DAG(
        dag_id="e-pavlov-8_hw3-4",
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['e-pavlov-8']
)

sunday = DummyOperator(
    task_id='sunday',
    dag=dag
)

today_date = textwrap.dedent("""{{ ds }}""")
# datetime.strptime(exec_date, '%d %B, %Y')


def check_day_of_the_week_func(exec_date):
    if datetime.strptime(str(exec_date), '%Y-%m-%d').weekday() == 6:
        return 'sunday'
    else:
        return 'get_article_heading'


check_day_of_the_week = BranchPythonOperator(
    task_id='check_day_of_the_week',
    python_callable=check_day_of_the_week_func,
    op_args=[today_date],
    provide_context=True,
    dag=dag
)


def get_article_heading_func(exec_date):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    cursor.execute('SELECT heading FROM articles WHERE id = {id}'.format(
        id=str(datetime.strptime(str(exec_date), '%Y-%m-%d').weekday()+1)
    ))  # исполняем sql
    # query_res = cursor.fetchall()  # полный результат
    logging.info(str(datetime.strptime(str(exec_date), '%Y-%m-%d')))
    logging.info(str(datetime.strptime(str(exec_date), '%Y-%m-%d').weekday()+1))
    logging.info(cursor.fetchone()[0])  # если вернулось единственное значение


get_article_heading = PythonOperator( # получаем данные из ГринПлама
    task_id='get_article_heading',
    python_callable=get_article_heading_func,
    op_args=[today_date],
    provide_context=True,
    dag=dag
)


check_day_of_the_week >> [get_article_heading, sunday]

