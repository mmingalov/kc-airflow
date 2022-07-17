"""
Homework, part 1
"""

from airflow import DAG
from datetime import datetime
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2021, 11, 19),
    'end_date': datetime(2021, 12, 31),
    'owner': 'e.ilicheva-2',
    'poke_interval': 600
}

with DAG(dag_id="e.ilicheva-2-2nd_dag",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         tags=['e.ilicheva-2', 'second_homework', 'greenplum']) as dag:

    start = DummyOperator(task_id='start')

    def greenplum_load(**kwargs):
        execution_date = kwargs['execution_date']
        weekday = execution_date.weekday() + 1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(weekday))  # исполняем sql
        query_res = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info("Info obtained")
        kwargs['ti'].xcom_push(value=query_res, key='article')
        logging.info("Info loaded")

    heading_operator = PythonOperator(
        task_id='greenplum',
        python_callable=greenplum_load,
        provide_context=True,
        dag=dag
    )

    end = DummyOperator(task_id='end')

    start >> heading_operator >> end
