"""
Даг из урока #5 РАЗРАБОТКА СВОИХ ПЛАГИНОВ
Создайте в GreenPlum'е таблицу с названием "m-grushina-6_ram_location"
с полями id, name, type, dimension, resident_cnt.
С помощью API (https://rickandmortyapi.com/documentation/#location)
найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
Запишите значения соответствующих полей этих трёх локаций в таблицу.
resident_cnt — длина списка в поле residents.
"""


from airflow import DAG
from datetime import datetime

from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm-grushina',
    'poke_interval': 600
}

with DAG("m-grushina_gp",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['m-grushina']
         ) as dag:

    def get_result_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук

        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        weekday = datetime.now().weekday()+1
        logging.info(f"weekday: {weekday}")
        logging.info('SELECT heading FROM articles WHERE id = %s', (weekday,))
        cursor.execute('SELECT heading FROM articles WHERE id = %s', (weekday,))  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        if not query_res:
                logging.info('the result is empty')
        else:
                logging.info(query_res[0])
        conn.close
        return query_res

    get_result = PythonOperator(
        task_id='get_result',
        python_callable=get_result_func,
        dag=dag
    )

    dummy = DummyOperator(task_id="dummy")


    def is_workdays_func(execution_dt):
        logging.info(execution_dt)
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day < 6

    workdays_only = ShortCircuitOperator(
        task_id='workdays_only',
        python_callable=is_workdays_func,
        op_kwargs= {'execution_dt' : '{{ ds }}'}
    )

    end = BashOperator(
        task_id='end',
        bash_command='echo "THE END"',
        trigger_rule='one_success'
    )

    dummy >> workdays_only >> get_result >> end

