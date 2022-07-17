import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dateutil.utils import today
from airflow.providers.postgres.hooks import postgres
DEFAULT_ARGS = {
    'start_date': days_ago(12),
    'owner': 'd.zhirnov-8',
    'poke_interval' : 600
}





with DAG(
    dag_id='Tutorial_zhirnov',
    schedule_interval='0 6 * * 1-6',
    #schedule_interval='@once',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['DE']
) as dag:

    def get_greenplum_data(**kwargs):
        pg_hook = postgres.PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        week_day = today().weekday() + 1
        cursor.execute('SELECT heading FROM articles WHERE id = ' + str(week_day))

        query_res = cursor.fetchall()[0]
        kwargs['ti'].xcom_push(value=query_res, key='sql_result')


    p_operator = PythonOperator(
        task_id='get_greenplum_data',
        python_callable=get_greenplum_data

    )
    p_operator

