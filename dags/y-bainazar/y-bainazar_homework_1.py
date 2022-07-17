"""
Складываем курс валют в GreenPlum
"""
from datetime import datetime, date

from airflow import DAG
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'y-bainazar',
    'poke_interval': 600
}

dag = DAG("y-bainazar_homework_1",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['y-bainazar']
          )


def todays_date():
    dt = str(date.today())
    exec_day = datetime.strptime(dt, '%Y-%m-%d').weekday()
    return exec_day + 1


def monday_saturday_func(execution_dt):
    exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
    return exec_day in [0, 1, 2, 3, 4, 5]


monday_saturday = ShortCircuitOperator(
    task_id='monday_saturday',
    python_callable=monday_saturday_func,
    op_kwargs={'execution_dt': '{{ ds }}'},
    dag=dag
)


def take_article_from_gp_and_push_func(val, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    # val = str(todays_date())
    cursor.execute(f'SELECT heading FROM articles WHERE id = {val}')  # исполняем sql
    query_res = cursor.fetchall()  # полный результат
    logging.info('-----------------')
    logging.info(f'Weekday: {val}')
    logging.info(f'Heading: {query_res}')
    logging.info('-----------------')
    kwargs['ti'].xcom_push(value=str(query_res), key='day_heading')


take_article_from_gp_and_push = PythonOperator(
    task_id='take_article_from_gp',
    python_callable=take_article_from_gp_and_push_func,
    op_kwargs={'val': str(todays_date())},
    provide_context=True,
    dag=dag
)


# def explicit_push_func(**kwargs):
#     value_push = stringval
#     kwargs['ti'].xcom_push(value=value_push, key=value_push)


# explicit_push = PythonOperator(
#     task_id='explicit_push',
#     python_callable=explicit_push_func,
#     provide_context=True,
#     dag=dag
# )

monday_saturday >> take_article_from_gp_and_push
