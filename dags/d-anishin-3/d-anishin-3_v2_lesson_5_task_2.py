"""
Урок 5 Задание (вторая часть)
Доработка дага, созданного на уроке 4.
"""

# Импортируем необходимые библиотеки
import datetime
import logging
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# Задаем в переменных по умолчанию расписание работы дага.
DEFAULT_ARGS = {
    'start_date': days_ago(7),
    'owner': 'd-anishin-3',
    'poke_interval': 600
}

with DAG("d-anishin-3_v3_lesson_5_task_2",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['d-anishin-3', 'lesson_5_v3']
         ) as dag:

    def get_weekday_num_func():
        weekday_num = datetime.datetime.today().weekday() + 1  # +1 из-за особенности даты запуска airflow
        return weekday_num


    def connect_to_db_ext_heading_func(**kwargs):
        ti = kwargs['ti']
        weekday_num = ti.xcom_pull(task_ids='get_weekday_num')
        pg_hook = PostgresHook(postgres_conn_id="conn_greenplum")
        conn = pg_hook.get_conn()
        cursor = conn.cursor("greenplum_connection")
        cursor.execute(f"SELECT heading FROM articles WHERE id = {weekday_num}")
        query_res = cursor.fetchall()
        ti.xcom_push(value=query_res, key='weekday_num')


    start = DummyOperator(task_id='start')

    end = DummyOperator(task_id='end')

    get_weekday_num = PythonOperator(
        task_id='get_weekday_num',
        python_callable=get_weekday_num_func
    )

    connect_to_db_ext_heading = PythonOperator(
        task_id='connect_to_db_ext_heading',
        python_callable=connect_to_db_ext_heading_func,
        do_xcom_push=True
    )

    start >> get_weekday_num >> connect_to_db_ext_heading >> end
