"""
Решение заданий к урокам 3 и 4
"""

from airflow import DAG
from airflow.utils.dates import days_ago


from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import datetime

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-klimenko-4',
    'poke_interval': 600
}

with DAG("a-klimenko-4_full_dag",
          schedule_interval='0 0 * * 1-6', #MON-SAT
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-klimenko-4']
          ) as dag:

    start = DummyOperator(task_id='start')

    echo_ds = BashOperator(
        task_id='echo_a-klimenko-4',
        bash_command='echo {{ a-gajdabura }}',
        dag=dag
    )

    def today_day_func():
        today_day = datetime.date.today()
        print(f"сегодня {today_day}")

    today_day = PythonOperator(
        task_id='today_day',
        python_callable=today_day_func,
        dag=dag
    )

    def execution_date_func():
        number_day = datetime.datetime.today().weekday() + 1
        return number_day


    execution_date = PythonOperator(
        task_id='execution_date',
        python_callable=execution_date_func
    )


    def gp_connection_func(**kwargs):
        ti = kwargs['ti']
        number_day = ti.xcom_pull(task_ids='execution_date')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cur = conn.cursor("my_cur")
        cur.execute(f'SELECT heading FROM articles WHERE id = {number_day};')
        result = cur.fetchall()
        ti.xcom_push(value=result, key='article')


    gp_connection = PythonOperator(
        task_id='gp_connection',
        python_callable=gp_connection_func
    )

    start >> [echo_ds, today_day] >> execution_date >> gp_connection


