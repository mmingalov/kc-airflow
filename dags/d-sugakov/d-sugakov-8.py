"""
Организуем работу DAGa по расписанию

"""
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'd-sugakov',
    'poke_interval': 600
}

with DAG("d-sugakov",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['d-sugakov']
    ) as dag:
 
    start = DummyOperator(task_id="start",
                          dag=dag
                        )

    def is_weekdays_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day in [0, 1, 2, 3, 4, 5]

    weekdays_only = ShortCircuitOperator(
        task_id='weekdays_only',
        python_callable=is_weekdays_func,
        op_kwargs={'execution_dt': '{{ ds }}'}
    )

    def get_article_from_greenplum_func(article_id):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("article_cursor")
        logging.info("extract heading FROM articles")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {article_id}')
        one_string = cursor.fetchone()[0]
        return one_string

    get_article_from_greenplum = PythonOperator(
            task_id='get_article_from_greenplum',
            python_callable=get_article_from_greenplum_func,
            op_args=['{{ dag_run.logical_date.weekday() + 1 }}'],
            do_xcom_push=True,
            dag=dag
        )

    echo_dsugakov = BashOperator(
            task_id='echo_dsugakov',
            bash_command='echo {{ ds }}',
            dag=dag
        )

    def hello_world_func():
        logging.info("Hello World!")

    hello_world = PythonOperator(
            task_id='hello_world',
            python_callable=hello_world_func,
            dag=dag
        )

    echo_datetime_now = BashOperator(
        task_id='echo_datetime_now',
        bash_command='echo {{ macros.datetime.now() }}',
        dag=dag
    )

    start >> weekdays_only >> [get_article_from_greenplum, echo_dsugakov, hello_world, echo_datetime_now]

