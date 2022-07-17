"""
Загрузка статьи на текущий день
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(10),
    'owner': 'a-grigorev-3',
    'poke_interval': 600
}

dag = DAG("a-grigorev-3_articles_load",
          schedule_interval='0 0 * * MON-SAT',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-grigorev-3']
          )


def get_current_day_number(current_date):
    return datetime.strptime(current_date, "%Y-%m-%d").date().isoweekday()


get_current_day_number = PythonOperator(
    task_id='get_current_day_number',
    python_callable=get_current_day_number,
    op_kwargs={'current_date': "{{ a-gajdabura }}"},
    dag=dag
)


def get_article(current_day_number, **context):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor("article_heading_cursor")
    cursor.execute('SELECT heading FROM articles WHERE id = {0}'.format(current_day_number))
    heading = cursor.fetchone()[0]
    context['ti'].xcom_push(value=heading, key='today_heading')


get_article = PythonOperator(
    task_id='get_article',
    python_callable=get_article,
    op_kwargs={"current_day_number": "{{ti.xcom_pull('get_current_day_number')}}"},
    provide_context=True,
    dag=dag
)

get_current_day_number >> get_article

dag.doc_md = __doc__

get_current_day_number.doc_md = """Возвращает цифру текущего дня"""
get_article.doc_md = """в today_heading возвращает заголовок текущего дня"""
