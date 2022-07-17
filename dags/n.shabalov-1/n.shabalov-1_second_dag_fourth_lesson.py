"""
Это даг, Забирать из таблицы articles значение поля heading из строки с id,
равным дню недели execution_date
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import datetime as dt

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'n.shabalov-1',
    'poke_interval': 600
}

def greenpulm(**kwargs):
    # инициализируем хук
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()  # берём из него соединение
    # и именованный (необязательно) курсор
    cursor = conn.cursor("named_cursor_name")
    logging.info("Соединение установленно")
    execution_date = dt.datetime.today().weekday() + 1
    cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(
        execution_date))  # исполняем sql
    query_res = cursor.fetchone()[0]  # результат
    logging.info("Значение полученно")
    kwargs['ti'].xcom_push(value=query_res, key='article')  # запиcываем в Xcom

dag = DAG("n.shabalov-1_dag4_lesson",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['karpov', 'nshabalov']
          )

dummy_operator = DummyOperator(
    task_id='dummy_task',
    retries=1,
    dag=dag)

def_operator = PythonOperator(
    task_id='script_task',
    python_callable=greenpulm,
    dag=dag)

dummy_operator >> def_operator
