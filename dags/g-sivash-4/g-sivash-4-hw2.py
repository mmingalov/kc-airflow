"""
1. Работает с понедельника по субботу, но не по воскресеньям
2. Ходит в наш GreenPlum
3. Забирает из таблицы articles значение поля heading из строки с id, равным дню недели execution_date (понедельник=1, вторник=2, ...)
4. Складывает получившееся значение в XCom
"""
import logging

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'owner': 'g-sivash-4',
    'start_date': days_ago(5),
    'poke_interval': 600
}

dag = DAG("g-sivash-4-hw2",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1
          )

start = DummyOperator(task_id='start', dag=dag)


def get_heading_from_psql_func(**context):
    execution_date = context['execution_date']
    weekday = execution_date.weekday() + 1
    hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')
    result = cursor.fetchone()[0]
    context['ti'].xcom_push(value=result, key='heading_weekday')

get_heading_from_psql = PythonOperator(
    task_id='get_heading_from_psql',
    provide_context=True,
    python_callable=get_heading_from_psql_func,
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

start >> get_heading_from_psql >> end
