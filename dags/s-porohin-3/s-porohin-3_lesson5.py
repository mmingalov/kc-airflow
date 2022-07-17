"""
ETL урок 5
Работать с понедельника по субботу, но не по воскресеньям (вариант через расписание)
Ходить в наш GreenPlum (используем соединение 'conn_greenplum'. Вариант решения — PythonOperator с PostgresHook внутри)
Забирать из таблицы articles значение поля heading из строки с id, равным дню недели execution_date (понедельник=1)
Складывать получившееся значение в XCom
Результат работы будет виден в интерфейсе с XCom.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(14),
    'owner': 's-porohin-3',
    'poke_interval': 600
}

with DAG("s-porohin-3_lesson5",
         schedule_interval='0 4 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['s-porohin-3']
         ) as dag:

    def get_article_from_dwh_func(logical_date: datetime, ds: str):
        weekday = logical_date.weekday()+1
        print("a-gajdabura:", ds)
        print("dayofweek:", weekday)
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')
        result = cursor.fetchone()
        if result:
            return result[0]


    lesson5_task = PythonOperator(
        task_id='get_article_from_dwh',
        python_callable=get_article_from_dwh_func
    )
