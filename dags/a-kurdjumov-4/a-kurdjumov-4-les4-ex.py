"""
Забираем данные по Heading из GP и заносим в XCom
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-kurdjumov-4',
    'poke_interval': 600
}

with DAG("a-kurdjumov-4-les4-ex",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-kurdjumov-4']
          ) as dag:
    dummy = DummyOperator(task_id='dummy')
    
    def is_not_sunday(exec_dt, **kwargs):
        exec_day = datetime.strptime(exec_dt, '%Y-%m-%d').weekday()+1
        kwargs['ti'].xcom_push(value=str(exec_day), key='dow')#сразу запушим день недели в XCOM чтобы использовать его при запросе в GP
        return exec_day != 7

    except_sunday = ShortCircuitOperator(
        task_id='except_sunday',
        python_callable=is_not_sunday,
        op_kwargs={'exec_dt': '{{ a-gajdabura }}'}
    )

    def get_heading_and_push(**kwargs):
        dow = kwargs['ti'].xcom_pull(task_ids='except_sunday', key='dow') # забираем день недели для запроса

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  
        conn = pg_hook.get_conn()  
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {dow}')  
        res = cursor.fetchone()[0]

        kwargs['ti'].xcom_push(value=str(res), key='result') #отправляем резы в XCOM

    get_head_and_push = PythonOperator(
        task_id='get_head_and_push',
        python_callable=get_heading_and_push
    )

    except_sunday>> get_head_and_push
