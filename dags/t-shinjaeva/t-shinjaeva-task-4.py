"""
Даг работает с понедельника по субботу,
Ходить в GreenPlum и забирает из таблицы articles
значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)
"""
from airflow import DAG
import datetime as dt
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': dt.datetime(2022, 3, 1, 0, 0, 0),
    'end_date': dt.datetime(2022, 3, 14, 0, 0, 0),
    'owner': 't-shinjaeva',
    'poke_interval': 600
}

with DAG("t-shinjaeva-task-4",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['t-shinjaeva']
) as dag:

    def get_heading_by_article_id(article_id):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("gp_conn")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {article_id};')
        return cursor.fetchone()[0]

    get_heading = PythonOperator(
        task_id='get_heading',
        python_callable=get_heading_by_article_id,
        op_args=['{{logical_date.isoweekday()}}'],
        dag=dag
    )

    get_heading