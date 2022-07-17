from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from airflow import macros

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'n-kotenko-3',
    'poke_interval': 600
}

with DAG("second_hw",
          schedule_interval='00 11 * * MON-SAT',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['n-kotenko-3']
          ) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')


    def select_from_greenplum(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        weekday = kwargs['execution_date'].weekday() + 1
        logging.info(f'Day of the week today is number {weekday}')
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        kwargs['ti'].xcom_push(value=query_res, key=f'heading_{weekday}')

    xcom_from_gp = PythonOperator(
        task_id='xcom_from_gp',
        python_callable=select_from_greenplum,
        provide_context=True
    )

    start >> xcom_from_gp >> end
