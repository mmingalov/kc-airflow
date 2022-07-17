"""
DAG должен работать с понедельника по субботу, но не по воскресеньям (можно реализовать с помощью расписания или операторов ветвления)
Ходить в наш GreenPlum.
Вариант решения — PythonOperator с PostgresHook внутри
Используйте соединение 'conn_greenplum' в случае, если вы работаете из LMS либо настройте его самостоятельно в вашем личном Airflow.
Параметры соединения:

Host: greenplum.lab.karpov.courses
Port: 6432
DataBase: karpovcourses
Login: student
Password: Wrhy96_09iPcreqAS

Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)
Выводить результат работы в любом виде: в логах либо в XCom'е
Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года
"""

from airflow import DAG
from datetime import datetime
import logging
from airflow.operators.dummy_operator import DummyOperator

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'm-mingalov',
    'poke_interval': 600,
    'retries': 3,
    'retry_delay': 10,
    'priority_weight': 2
}

with DAG("m-mingalov_4_load_heading_to_xcom",
    schedule_interval='0 0 * * 1-6', # at 00:00 on every day-of-week from Monday through Saturday
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['m-mingalov']
    ) as dag:

    dummy = DummyOperator(task_id="dummy")

    def read_gp_to_xcom_func(day_of_week,**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        #wd = datetime.now().isoweekday()

        cursor = conn.cursor("named_cursor_name")

        query = f'SELECT heading FROM articles WHERE id = {day_of_week};'
        logging.info(query)
        cursor.execute(query)
        one_string = cursor.fetchone()[0]
        logging.info('PUT in XCOM: {one_string_}'.format(one_string_=one_string))
        kwargs['ti'].xcom_push(value=one_string, key='return_val_gp')

    read_gp_to_xcom_func = PythonOperator(
        task_id='read_gp_to_xcom',
        python_callable=read_gp_to_xcom_func,
        op_args=['{{ logical_date.weekday() + 1}}']
        #dag=dag
    )

    dummy >> read_gp_to_xcom_func

