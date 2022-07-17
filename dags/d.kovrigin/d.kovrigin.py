"""
Загрузка из articles
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import pendulum
import datetime

#from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {'start_date': pendulum.datetime(2022, 3, 1, tz="UTC")
                , 'end_date': pendulum.datetime(2022, 3, 14, tz="UTC")
                , 'owner': 'd.kovrigin'
                , 'poke_interval': 600
                }

with DAG(dag_id="dag_kovrigin",
    catchup=True,
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['d.kovrigin']
    ) as dag:

        def load_sql_func():
            pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
            conn = pg_hook.get_conn()  # берём из него соединение
            cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
            cursor.execute('SELECT heading FROM articles WHERE id = 1')  # исполняем sql
            query_res = cursor.fetchall()  # полный результат
            one_string = cursor.fetchone()[0]
            logging.info(query_res)
            logging.info(one_string)

        load_sql = PythonOperator(
                        task_id='load_sql',
                        python_callable=load_sql_func,
                        dag=dag)



load_sql
