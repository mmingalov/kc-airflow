"""
забираем данные с  GreenPlum и выводим в лог
"""

from airflow import DAG
import logging
import datetime
#######

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
import pendulum
from datetime import datetime


DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz='Europe/Moscow'),
    'end_date': pendulum.datetime(2022, 4, 15, tz='Europe/Moscow'),
    'owner': 'a-kovalev-8',
    'poke_interval': 60
}

with DAG("select_from_gp_2",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-kovalev-8']
         ) as dag:

    def select_from_gp_func(**kwargs):
        this_date = kwargs['ds']
        day_of_week = datetime.strptime(this_date, '%Y-%m-%d').weekday() + 1

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук

        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')
        one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info(one_string)

    select_from_gp = PythonOperator(
        task_id='select_from_gp',
        python_callable=select_from_gp_func,
        provide_context=True
    )


    def logging_func_start():
        logging.info("dag started")


    start_operator = PythonOperator(
        task_id='start_operator',
        python_callable=logging_func_start
    )

    def logging_func_end():
        logging.info("dag end")

    end_operator = PythonOperator(
        task_id='end_operator',
        python_callable=logging_func_end
    )

    start_operator >> select_from_gp >> end_operator