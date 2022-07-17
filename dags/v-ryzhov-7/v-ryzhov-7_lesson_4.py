"""
v-ryzhov-7-lesson4
Работать с понедельника по субботу, но не по воскресеньям
Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)
Выводить результат работы в любом виде: в логах либо в XCom'е

"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.weekday import DayOfWeekSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
        'start_date': days_ago(35),
        'owner': 'v-ryzhov-7_lesson_4',
        'email': ['vryzhov@prosv.ru'],
        'poke_interval': 600
    }

with DAG("v-ryzhov-7_lesson_4",
        schedule_interval='0 0 * * 1-6',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['v-ryzhov-7_lesson_4']
    ) as dag:

    dummy = DummyOperator(task_id="dummy")

    def get_article_by_id(article_id):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("gp_conn")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {article_id};')
        return cursor.fetchone()[0]

    # get_article
    get_article = PythonOperator(
        task_id='get_article',
        python_callable=get_article_by_id,
        op_args=['{{ dag_run.logical_date.weekday() + 1 }}', ],
        do_xcom_push=True,
        dag=dag
    )

    dummy >> get_article
