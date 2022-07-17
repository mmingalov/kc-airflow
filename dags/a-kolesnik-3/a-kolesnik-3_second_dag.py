"""
The DAG with GP connection.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import datetime

DEFAULT_ARGS = {
    'start_date': days_ago(7),
    'owner': 'a-kolesnik-3',
    'poke_interval': 600
}

with DAG("a-kolesnik-3_second_dag",
         schedule_interval='0 0 * * 1-6',  # works on Mon-Sat, not Sun
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-kolesnik-3']
         ) as dag:
    begin = DummyOperator(task_id='begin')


    def get_weekday_num():
        weekday_num = datetime.datetime.today().isoweekday()  # Mon=1, Sun=7
        return weekday_num


    exec_date = PythonOperator(
        task_id='exec_date',
        python_callable=get_weekday_num,
        dag=dag
    )


    def gp_connect_func(**kwargs):
        weekday_num = kwargs['ti'].xcom_pull(task_ids='exec_date')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        c = conn.cursor("my_cursor")
        c.execute(f'SELECT heading FROM articles WHERE id = {weekday_num}')
        result = c.fetchall()
        kwargs['ti'].xcom_push(value=result, key='article')


    gp_connect = PythonOperator(
        task_id='gp_connect',
        python_callable=gp_connect_func,
        dag=dag
    )

    end = DummyOperator(task_id='end')

    begin >> exec_date >> gp_connect >> end
