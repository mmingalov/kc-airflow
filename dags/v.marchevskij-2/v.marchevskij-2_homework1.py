from datetime import timedelta, datetime

import airflow
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'owner': 'v.marchevskij-2',
    'depends_on_past': False,
    'wait_for_downstream': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': airflow.utils.dates.days_ago(0),
    'end_date': datetime(2021, 12, 13),
    'trigger_rule': 'all_success'
}

with DAG(
    dag_id='marchevskiy_homework2',
    description='A homework 2 DAG.',
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1
) as dag:

    start = DummyOperator(task_id='start')

    def get_execution_weekday(**kwargs):
        """
        Get execution day number (monday=1 .. sunday=7) and push it into xcom.
        """
        ex_date = datetime.today().weekday() + 1  # add 1 because datetim "Return the day of the week as an integer,
        # where Monday is 0 and Sunday is 6."
        # https://docs.python.org/3/library/datetime.html#datetime.datetime.weekday
        kwargs['ti'].xcom_push(value=ex_date, key='execution_weekday')

    execution_weekday_push = PythonOperator(
        task_id='execution_weekday_push',
        python_callable=get_execution_weekday,
        provide_context=True
    )

    def get_article_heading_func(**kwargs):
        ex_date = kwargs['ti'].xcom_pull(task_ids='execution_weekday_push', key='execution_weekday')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {ex_date}')
        query_res = cursor.fetchone()[0]
        kwargs['ti'].xcom_push(value=query_res, key='article_heading')

    get_article_heading = PythonOperator(
        task_id='get_article_heading',
        python_callable=get_article_heading_func,
        provide_context=True
    )

    start >> execution_weekday_push >> get_article_heading
