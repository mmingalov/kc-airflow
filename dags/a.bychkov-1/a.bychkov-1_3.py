"""
Выполнение 2 практического задания.
"""
import datetime
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a.bychkov-1',
    'poke_interval': 600
}

with DAG("a.bychkov-1_hw_2",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a.bychkov-1']
         ) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    def is_sunday():
        exec_day = datetime.datetime.today().weekday() + 1
        return exec_day

    weekday_only = PythonOperator(
                                  task_id='weekday_only',
                                  python_callable=is_sunday
                                  )
    
    def GetDataFromGreenplum(**kwargs):
        ti = kwargs['ti']
        exec_day = ti.xcom_pull(task_ids='weekday_only')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {exec_day};')
        query_res = cursor.fetchall() 
        #one_string = cursor.fetchone()[0]
        result = ti.xcom_push(value=query_res, key='article')

    GetData = PythonOperator(
                            task_id='GetDataFromGreenplum',
                            python_callable=GetDataFromGreenplum,
                            provide_context=True
                            )

    start >> weekday_only >> GetData >> end