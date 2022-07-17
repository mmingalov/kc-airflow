"""
Строим DAG, который должен:

    Работать с понедельника по субботу, но не по воскресеньям

    Ходить в GreenPlum

    Забирать из таблицы articles значение поля heading из строки с id, равным дню недели execution_date (понедельник=1, вторник=2, ...)

    Складывать получившееся значение в XCom

Результат работы будет виден в интерфейсе с XCom.
"""

from airflow import DAG
from datetime import datetime
import logging


from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 29),
    'owner': 'a_gajdabura_plugins',
    'poke_interval': 600,
    'retries': 3,
    'retry_delay': 10,
    'priority_weight': 2
}

with DAG("a-gajdabura_load_heading_to_xcom",
          schedule_interval='0 18 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a_gajdabura_plugins']
          ) as dag:

    def load_heading_to_xcom_func(current_date, **kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("get_heading_cursor")
        logging.info("SELECT heading FROM articles WHERE id = '{}'"\
                     .format(datetime.strptime(current_date, "%Y-%m-%d").date().isoweekday()))
        cursor.execute("SELECT heading FROM articles WHERE id = 1")
        one_string = cursor.fetchone()[0]
        conn.close()
        kwargs['ti'].xcom_push(value=one_string, key='today_heading')


    load_heading_to_xcom = PythonOperator(
        task_id='load_heading_to_xcom',
        op_kwargs={'current_date': "{{ ds }}"},
        python_callable=load_heading_to_xcom_func,
        provide_context=True
    )

load_heading_to_xcom

