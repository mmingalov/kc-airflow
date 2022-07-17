"""
1.Работать с понедельника по субботу, но не по воскресеньям (можно реализовать с помощью расписания или операторов ветвления)

2. Ходить в наш GreenPlum. Вариант решения — PythonOperator с PostgresHook внутри
3. Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)
4. Выводить результат работы в любом виде: в логах либо в XCom'е

Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года

"""
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.sensors.weekday import DayOfWeekSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'i-orlov-7',
    'poke_interval': 600,
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14)
}

with DAG("i-orlov-7_article_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['i-orlov-7']
         ) as article_dag:

    start = DummyOperator(task_id='start')

    def is_weekday_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day not in [7]

    weekday_only = ShortCircuitOperator(
        task_id='weekday_only',
        python_callable = is_weekday_func,
        op_kwargs={'execution_dt': '{{ ds }}'},
        dag=article_dag
        )

    def get_article_by_id(article_id):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("gp_conn")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {article_id};')
        return cursor.fetchone()[0]

    get_article = PythonOperator(
        task_id='get_article',
        python_callable=get_article_by_id,
        op_args=['{{ dag_run.logical_date.weekday() + 1 }}', ],
        do_xcom_push=True,
        dag=article_dag
        )

    start >> weekday_only >> get_article
