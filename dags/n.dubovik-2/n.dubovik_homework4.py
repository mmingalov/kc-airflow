"""
Даг забирает из GreenPlum значение поля 'heading' в таблице 'articles'.
Значение поля 'heading' берется из строки с id, равным дню недели.
Получившееся значение кладется в XCom.
"""

import logging
from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'owner': 'n.dubovik-2',
    'email': ['nikolaidubovik@gmail.com'],
    'start_date': datetime(2021, 11, 1),
    'poke_interval': 300,
    'retries': 3
}

with DAG(
        dag_id='n.dubovik_homework4',
        schedule_interval='0 0 * * 1-6',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['n.dubovik-2']
) as dag:

    def load_greenplum(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        logging.info("Соединение с GreenPlum установленно")

        execution_date = datetime.today().weekday() + 1
        cursor.execute(f'SELECT heading FROM articles WHERE id = {execution_date}')
        query_res = cursor.fetchone()[0]
        logging.info("Значение поля 'heading' полученно")

        kwargs['ti'].xcom_push(value=query_res, key='id=' + str(execution_date))
        logging.info("Значение поля 'heading' записано в Xcom")


    start_time = BashOperator(
        task_id='print_start_time',
        bash_command='echo {{ a-gajdabura }}'
    )

    load_greenplum = PythonOperator(
        task_id='load_greenplum',
        python_callable=load_greenplum
    )

    finish_time = BashOperator(
        task_id='print_finish_time',
        bash_command='echo {{ a-gajdabura }}'
    )

start_time >> load_greenplum >> finish_time

dag.doc_md = __doc__
start_time.doc_md = """Пишет в логи время запуска DAG'а"""
load_greenplum.doc_md = """Забирает из GreenPlum значение поля 'heading' в таблице 'articles'.
                           Значение поля 'heading' берется из строки с id, равным дню недели.
                           Получившееся значение кладется в XCom."""
finish_time.doc_md = """Пишет в логи время окончания DAG'а"""
