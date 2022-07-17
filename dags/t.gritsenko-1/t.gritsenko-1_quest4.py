from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
import datetime

default_args = {
    'start_date': days_ago(0),
    'owner': 'Karpov',
    'poke_interval': 600
}

with DAG("t.gritsenko-1_test",
         schedule_interval='0 0 * * 1-6',
         default_args=default_args,
         max_active_runs=1,
         tags=['karpov']
         ) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    def execution_date_func():
        number_day = datetime.datetime.today().weekday() + 1
        return number_day


    execution_date = PythonOperator(
        task_id='execution_date',
        python_callable=execution_date_func
    )

    def gp_conn_func(**kwargs):
        ti = kwargs['ti']
        number_day = ti.xcom_pull(task_ids='execution_date')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cur = conn.cursor("named_cursor_name")
        cur.execute(f'SELECT heading FROM articles WHERE id = {number_day};')
        result = cur.fetchall()
        query_result = ti.xcom_push(value=result, key='article')


    gp_conn = PythonOperator(
        task_id='gp_conn',
        python_callable=gp_conn_func
    )

    start >> execution_date >> gp_conn >> end
