from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from airflow import macros
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'v.pismennyj-2',
    'poke_interval': 600
}

with DAG("v.pysmennyi_dag2",
         schedule_interval='0  0  * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['v.pismennyj-2']
         ) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    def connect_to_gp(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        weekday = macros.datetime.today().weekday()
        logging.info(f'Today is {weekday}')
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')
        query_res = cursor.fetchall()
        logging.info(query_res)
        kwargs['ti'].xcom_push(value=query_res, key=f'heading_{weekday}')

    get_data = PythonOperator(
        task_id='get_data',
        python_callable=connect_to_gp,
        provide_context=True
    )

    start >> get_data >> end