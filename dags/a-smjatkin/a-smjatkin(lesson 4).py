"""
Работать с понедельника по субботу, но не по воскресеньям (можно реализовать с помощью расписания или операторов ветвления)
Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)
Выводить результат работы в любом виде: в логах либо в XCom'е
"""

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 5, 1),
    'end_date': datetime(2022, 5, 15),
    'owner': 'a-smjatkin',
    'poke_interval': 600,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
schedule_interval = '0 0 * * 1-6'


@dag(dag_id='a-smiatkin-4_articles_GP', tags=['a-smjatkin'], default_args=DEFAULT_ARGS, catchup=True,
     schedule_interval=schedule_interval)
def get_data_from_gp_heading():
    # Получаем данные из GreenPlum за соответствующий день недели
    @task()
    def get_data_from_gp():
        wd = datetime.now().isoweekday()
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("gp_conn")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {wd};')
        db_result = cursor.fetchall()
        return db_result

    # выводим на печать в лог
    @task()
    def print_data_to_log(total):
        logging.info(total)

    print_data_to_log(get_data_from_gp())


a_smjatkin_dag = get_data_from_gp_heading()
