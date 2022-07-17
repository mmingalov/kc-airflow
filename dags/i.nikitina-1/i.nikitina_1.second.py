"""
Задание 4 урока
Работа с GreenPlum
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'i.nikitina-1',
    'poke_interval': 600
}

with DAG("i.nikitina-1.second",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['karpov']
          ) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    def greenplum_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        week_day = datetime.datetime.today().weekday() + 1
        cursor.execute(f'SELECT heading FROM articles WHERE id = {week_day}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        #one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        res=kwargs['ti'].xcom_push(value=query_res, key='article')

    greenplum = PythonOperator(
        task_id='greenplum',
        python_callable=greenplum_func
    )

    start >> greenplum >> end

