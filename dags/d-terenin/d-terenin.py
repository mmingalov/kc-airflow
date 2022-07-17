"""
Нужно доработать даг, который вы создали на прошлом занятии.

Он должен:

Работать с понедельника по субботу, но не по воскресеньям (можно реализовать с помощью расписания или операторов ветвления)

Ходить в наш GreenPlum. Вариант решения — PythonOperator с PostgresHook внутри

Используйте соединение 'conn_greenplum' в случае, если вы работаете из LMS либо настройте его самостоятельно в вашем личном Airflow.

Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)
Выводить результат работы в любом виде: в логах либо в XCom'е
Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года
"""

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import pendulum
import logging



DEFAULT_ARGS = {
    'start_date': days_ago(51),
    #'start_date': pendulum.datetime(2022, 3, 1, tz="UTC"),
    #'end_date': pendulum.datetime(2022, 3, 14, tz="UTC"),
    'owner': 'd-terenin',
    'poke_interval': 600
}

with DAG("d-terenin_gp_article",
         schedule_interval="0 0 * * 1-6",  # At 00:00 on every day-of-week from Monday through Saturday.
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['d-terenin']
         ) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")


    def get_from_greenplum(article_id):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("article_cursor")
        logging.info("extract heading FROM articles")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {article_id}')
        result = cursor.fetchone()[0]
        return result

    push_data_xcom = PythonOperator(
        task_id='get_article',
        python_callable=get_from_greenplum,
        op_args=['{{ dag_run.logical_date.weekday() + 1 }}'],
        do_xcom_push=True,
        dag=dag
    )

    start >> push_data_xcom >> end
