from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

import logging
# from airflow.providers.postgres.hook.postgres import PostgresHook


DEFAULT_ARGS = {
    'start_date':        datetime(2022, 3, 1),
    # 'end_date':          datetime(2022, 3, 15),
    'owner':             'p-evtuhovich-8',
    'e-mail':            'p.evtuhovich.pos.credit@gmail.com'
}


def get_from_gp(**kwargs):
    day_from = datetime.weekday(datetime.strptime(kwargs['day_run'], '%Y-%m-%d')) + 1
    pg_hook = PostgresHook("conn_greenplum")
    conn = pg_hook.get_conn()
    cursor = conn.cursor("named_cursor_name")
    cursor.execute(f"SELECT heading FROM articles WHERE id = {day_from}")
    query_res = cursor.fetchall()
    # one_string = cursor.fetchone()[0]
    kwargs['ti'].xcom_push(key='query_res',
                           value=query_res)


def print_log(**kwargs):
    logging.info(f"----------------{kwargs['day_run']}-------------------")
    logging.info(kwargs['ti'].xcom_pull(task_ids='p_evtuhovich_8_get_from_gp',
                                        key='query_res'))
    logging.info('-------------------------------------------')


with DAG("p_evtuhovich_8_test",
         schedule_interval='10 15 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         catchup=True,
         tags=['p-evtuhovich-8']
         ) as dag:
    p_evtuhovich_8_get_from_gp = PythonOperator(
            task_id='p_evtuhovich_8_get_from_gp',
            python_callable=get_from_gp,
            provide_context=True,
            op_kwargs={'day_run': '{{ ds }}'},
            dag=dag
    )

    p_evtuhovich_8_print_arg = PythonOperator(
            task_id='p_evtuhovich_8_print_arg',
            python_callable=print_log,
            provide_context=True,
            op_kwargs={'day_run': '{{ ds }}'},
            dag=dag
    )

p_evtuhovich_8_get_from_gp >> p_evtuhovich_8_print_arg
