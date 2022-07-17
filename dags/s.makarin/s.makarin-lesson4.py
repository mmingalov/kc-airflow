"""
Идем в greenplum, забираем из таблицы articles
значение поля heading из строки с id = дню недели и
выводим результат работы в виде логов
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag, task
from datetime import datetime
import logging


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'owner': 's.makarin',
    'poke_interval': 600
}

schedule_interval = '* * * * 1-6'

@dag(default_args = DEFAULT_ARGS,
     catchup=True,
     schedule_interval = schedule_interval,
     tags=['s.makarin'])
def get_data_from_gp_heading():

    #получаем сегодняшний день недели (1 - понедельник, 2 - вторник...)
    @task()
    def day_now():
        exec_date = '{{ds}}'
        weekday = datetime.strftime(exec_date, '%w')
        return weekday

    #Получаем данные из GreenPlum за соответствующий день
    @task()
    def get_data_from_gp(daynow):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute('SELECT heading FROM articles WHERE id = {day}'.format(day=daynow))
        db_result = cursor.fetchone()[0]
        #query_res = cursor.fetchall()
        return db_result
    #выводим на печать в лог
    @task()
    def print_data_to_log(total):
        logging.info(total)

    print_data_to_log(get_data_from_gp(day_now()))

s_makarin_l4_dag = get_data_from_gp_heading()


