"""
Забирает данные поля heading из таблицы articles с id равным дню недели
Работает с понедельника по субботу в 2:00
"""
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(5),
    'owner': 'a-zhukov-6',
    'poke_interval': 600
}

with DAG("a_zhukov_practice4",
         schedule_interval='0 2 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-zhukov-6']
         ) as dag:

    def load_articles_from_greenplum_func(date, **context):
        week_day = datetime.strptime(date, "%Y-%m-%d").weekday() + 1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {week_day}')
        query_res = cursor.fetchall()
        context['ti'].xcom_push(key='articles_by_weekday', value=query_res)


    load_articles_from_greenplum = PythonOperator(
        task_id='load_articles_from_greenplum',
        python_callable=load_articles_from_greenplum_func,
        op_kwargs={'date': '{{ ds }}'},
        dag=dag
    )
