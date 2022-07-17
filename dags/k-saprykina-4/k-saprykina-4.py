"""
Тестовый даг, который забирает
заголовки статей из Greenplum
и скалдывает в XCom
"""

import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'k-saprykina-4',
    'poke_interval': 600
}

with DAG("k-saprykina-4",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['k-saprykina-4']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")

    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo Сейчас мы будем грузить заголовки статей из Greenplum в XCom',
        dag=dag
    )

    def go_to_greenplum(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        weekday = datetime.datetime.today()
        cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(weekday.weekday() + 1))  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        kwargs['ti'].xcom_push(value=query_res, key='heading')  # cкладываем получившееся значение в XCom

    greenplum_task = PythonOperator(
        task_id='greenplum_connection',
        python_callable=go_to_greenplum,
        dag=dag
    )

    dummy >> [bash_task, greenplum_task]

