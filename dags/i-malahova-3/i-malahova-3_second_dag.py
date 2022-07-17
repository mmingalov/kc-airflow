"""
Это второй даг.
Он проводит псевдо-жеребьевку, выбирает принимающую команду и команду-гостя
Потом идет в Greenplum и забирает строку с id, равному дню недели
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import random
import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(12),
    'owner': 'i-malahova-3',
    'poke_interval': 600
}

TEAMS_LIST = [
    'Зенит',
    'Кузбасс',
    'Динамо',
    'Локомотив',
    'Урал',
    'Искра'
]

with DAG("i-malahova-3_second_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['malahova']
         ) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    def is_sunday_func():
        exec_day = datetime.datetime.today().weekday()
        return exec_day in [0, 1, 2, 3, 4, 5]


    from_monday_to_saturday = ShortCircuitOperator(
        task_id='from_monday_to_saturday',
        python_callable=is_sunday_func,
        op_kwargs={'execution_dt': '{{ a-gajdabura }}'}
    )


    def choose_host():
        logging.info(random.choice(TEAMS_LIST))


    host = PythonOperator(
        task_id='host',
        python_callable=choose_host
    )


    def choose_guest():
        logging.info(random.choice(TEAMS_LIST))

    guest = PythonOperator(
        task_id='guest',
        python_callable=choose_guest
    )

    def go_to_greenplum(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        exec_day = datetime.datetime.today()
        cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(exec_day.weekday() + 1))  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        kwargs['ti'].xcom_push(value=query_res, key='heading')
        logging.info('Number {weekday} is added')

    greenplum_conn = PythonOperator(
        task_id='greenplum_conn',
        python_callable=go_to_greenplum
    )

start >> from_monday_to_saturday >> host >> guest >> greenplum_conn >> end



