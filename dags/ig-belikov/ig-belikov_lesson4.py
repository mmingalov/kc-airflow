"""
Идем в greenplum, забираем из таблицы articles
значение поля heading из строки с id = дню недели и
выводим результат работы в виде логов
"""
from datetime import datetime
from airflow.decorators import dag, task
import logging
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'owner': 'i.belikov',
    'poke_interval': 600,
    'depends_on_past': False
}


@dag("ig-belikov_lesson4",
     schedule_interval='0 3 * * 1-6',
     default_args=DEFAULT_ARGS,
     max_active_runs=1,
     tags=['i.belikov']
     )
def task_flow():

    @task
    def get_data_from_gp(**kwargs):
        ds = datetime.strptime(kwargs['ds'], '%Y-%m-%d')
        weekday = ds.isoweekday()

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        with pg_hook.get_conn() as conn:
            cur = conn.cursor()
            cur.execute(f'SELECT HEADING FROM ARTICLES WHERE ID = {weekday}')
            if cur is not None:
                resultset = cur.fetchone()[0]
                logging.info(f'RESULT_SET IS: {resultset}')

    get_data_from_gp()


dag = task_flow()
