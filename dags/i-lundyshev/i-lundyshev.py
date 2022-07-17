"""
Данный DAG после доработки забирает  из таблицы articles значение поля heading из строки с id,
равным дню недели ds (понедельник=1, вторник=2, ...) и выводит это значение в лог
Срабатывает с понедельника по субботу.
Даты работы DAG: с 1 марта 2022 года по 14 марта 2022 года.

"""
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
import logging
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import timedelta, datetime

DEFAULT_ARGS = {
    #'start_date': days_ago(36),
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'i-lundyshev',
    'poke_interval': 600,
    'depends_on_past': False
}

day_week = "{{execution_date.strftime('%w')}}"

with DAG("i-lundyshev",
        schedule_interval='3 3 * * 1-6',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['i-lundyshev']
        ) as dag:

    def get_heading_from_articles (day_week):
        #day_week = datetime.now().weekday() + 1
        #day_week = datetime.strptime({{ ds }}, '%Y-%m-%d').weekday()
        #day_week = "{{ execution_date.strftime('%w') }}"

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("i-lundyshev_cursor")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_week}')  # исполняем sql
        one_string = cursor.fetchone()[0]
        logging.info(one_string)
        conn.close()

    get_heading = PythonOperator(
        task_id='get_heading',
        python_callable=get_heading_from_articles,
        op_args=[day_week],
        dag=dag
    )

get_heading

dag.doc_md = __doc__

#dummy.doc_md = """Пустышка для начала DAG"""
get_heading = """Пишет в лог заголовок статьи с id равным дню недели"""


