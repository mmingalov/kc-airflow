from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import datetime

DEFAULT_ARGS = {
    'start_date': days_ago(12),
    'owner': 'd.zharkynbek-2',
    'poke_interval': 600
}

with DAG("dindar_2_dag",
         start_date=days_ago(1),
         schedule_interval="0 0 * * MON-SAT",
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['d.zharkynbek-2']
         ) as dag:

    start = DummyOperator(task_id='start')

    def get_heading_from_articles():
        week_day = datetime.datetime.today().weekday() + 1 # get weekday value
        pg_hook = PostgresHook('conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute('SELECT heading FROM articles WHERE id = ' + str(week_day))  # исполняем sql
        one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        return one_string  # push value to xcom

    from_articles_to_xcom = PythonOperator(
        task_id='get_heading_from_articles',
        python_callable=get_heading_from_articles
    )

    start >> from_articles_to_xcom
