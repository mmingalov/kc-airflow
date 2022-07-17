import logging
from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor


shchelokovskiy_articles_dag = DAG(
    dag_id='shchelokovskiy_articles_dag',
    start_date=pendulum.datetime(2022, 3, 1, tz='UTC'),
    schedule_interval='0 0 * * 1-6',
    tags=['shchelokovskiy']
)


def extract_and_load_articles_heading(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    connection = pg_hook.get_conn()
    cursor = connection.cursor(
        "articles_cursor",
        cursor_factory=RealDictCursor,
    )

    execution_date = datetime.strptime(
        kwargs['execution_date'],
        '%Y-%m-%d',
    )
    day_number = execution_date.weekday() + 1
    cursor.execute(f'SELECT heading FROM articles WHERE id = {day_number}')
    articles_heading = cursor.fetchall()
    logging.info(f'day_number: {day_number}')
    logging.info(f'articles_heading: {articles_heading}')
    ti = kwargs['ti']
    ti.xcom_push('articles_heading', articles_heading)


extract_articles_heading_task = PythonOperator(
    task_id='shchelokovskiy_extract_articles_heading_task',
    python_callable=extract_and_load_articles_heading,
    dag=shchelokovskiy_articles_dag,
    op_kwargs={'execution_date': '{{ds}}'},
)

extract_articles_heading_task
