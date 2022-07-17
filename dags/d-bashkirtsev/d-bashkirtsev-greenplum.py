"""
DAG по заданию №2 блока ETL

1. Работает с понедельника по субботу, но не по воскресеньям
2. Забирает из таблицы articles значение поля heading из строки с id, равным дню недели execution_date (понедельник=1, вторник=2, ...)
3. Складывает получившееся значение в XCom
"""

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
import datetime

def get_data_from_gp_and_push_to_xcom(**kwargs):
    day_load = datetime.datetime.now().weekday()+1

    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor("d_bashkirtsev_cursor")
    cursor.execute(f'SELECT heading FROM articles WHERE id = {day_load}')  # исполняем sql
    one_string = cursor.fetchone()[0]
    kwargs['ti'].xcom_push(value=one_string, key='pushed_value')

DEFAULT_ARGS = {
    'owner': 'd-bashkirtsev',
    'start_date': days_ago(0),
    'poke_interval': 600
}

dag = DAG("d-bashkirtsev-greenplum",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['bashkirtsev', 'lesson_4'],
          )

start = DummyOperator(task_id='start', dag=dag)

get_heading_from_psql = PythonOperator(
    task_id='get_data_from_gp_and_push_to_xcom',
    provide_context=True,
    python_callable=get_data_from_gp_and_push_to_xcom,
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

start >> get_heading_from_psql >> end

