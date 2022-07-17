from airflow import DAG

from airflow.utils.dates import days_ago
import logging

from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import ShortCircuitOperator
from datetime import datetime
from datetime import date

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'g.bashanov-4',
    'poke_interval': 600
}

with DAG(
    dag_id="g.bashanov-4_plum_xcom",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['g.bashanov-4']
) as dag:

    def not_sunday_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        logging.info(exec_day)
        return exec_day in [0, 1, 2, 3, 4, 5]

    not_sunday = ShortCircuitOperator(
       task_id='not_sunday', 
       python_callable=not_sunday_func, 
       op_kwargs={'execution_dt': '{{ a-gajdabura }}'}
    )

    def hook_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        exec_day = date.today()
        cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(exec_day.weekday()))  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        kwargs['ti'].xcom_push(value=query_res, key='heading')

    hook_pg = PythonOperator(
        task_id='hook_pg',
        python_callable=hook_func
    )

    not_sunday >> hook_pg